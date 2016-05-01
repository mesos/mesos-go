package app

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/backoff"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/scheduler/calls"
	"github.com/mesos/mesos-go/scheduler/events"
)

func Run(cfg Config) error {
	log.Printf("scheduler running with configuration: %+v", cfg)

	state, err := newInternalState(cfg)
	if err != nil {
		return err
	}
	var (
		frameworkInfo      = buildFrameworkInfo(cfg)
		subscribe          = calls.Subscribe(true, frameworkInfo)
		registrationTokens = backoff.Notifier(1*time.Second, 15*time.Second, nil)
		handler            = buildEventHandler(state)
	)
	for {
		state.metricsAPI.subscriptionAttempts()
		resp, opt, err := state.cli.Call(subscribe)
		func() {
			if resp != nil {
				defer resp.Close()
			}
			if err == nil {
				err = state.cli.WithTemporary(opt, func() error {
					return eventLoop(state, resp.Decoder(), handler)
				})
			}
			if err != nil && err != io.EOF {
				state.metricsAPI.apiErrorCount("subscribe")
				log.Println(err)
			} else {
				log.Println("disconnected")
			}
		}()
		if state.done {
			return state.err
		}
		if frameworkInfo.GetFailoverTimeout() > 0 && state.frameworkID != "" {
			subscribe.Subscribe.FrameworkInfo.ID = &mesos.FrameworkID{Value: state.frameworkID}
		}
		<-registrationTokens
		log.Println("reconnecting..")
	}
}

// eventLoop returns the framework ID received by mesos (if any); callers should check for a
// framework ID regardless of whether error != nil.
func eventLoop(state *internalState, eventDecoder encoding.Decoder, handler events.Handler) (err error) {
	state.frameworkID = ""
	for err == nil && !state.done {
		var e scheduler.Event
		if err = eventDecoder.Invoke(&e); err == nil {
			if state.config.verbose {
				log.Printf("%+v\n", e)
			}
			err = handler.HandleEvent(&e)
		}
	}
	return err
}

// buildEventHandler generates and returns a handler to process events received from the subscription.
func buildEventHandler(state *internalState) events.Handler {
	callOptions := scheduler.CallOptions{} // should be applied to every outgoing call
	// TODO(jdef) refactor per-event metrics as a generic, named counter; build generic functional wrapper that
	// increments the count and apply the wrapper to each handler.
	return events.NewMux(
		events.Handle(scheduler.Event_FAILURE, events.HandlerFunc(func(e *scheduler.Event) error {
			state.metricsAPI.failuresReceived()
			log.Println("received a FAILURE event")
			f := e.GetFailure()
			failure(f.ExecutorID, f.AgentID, f.Status)
			return nil
		})),
		events.Handle(scheduler.Event_OFFERS, events.HandlerFunc(func(e *scheduler.Event) error {
			if state.config.verbose {
				log.Println("received an OFFERS event")
			}
			offers := e.GetOffers().GetOffers()
			state.metricsAPI.offersReceived.Int(len(offers))
			t := time.Now()
			resourceOffers(state, callOptions[:], offers)
			if state.config.summaryMetrics {
				state.metricsAPI.processOffersLatency.Since(t)
			}
			return nil
		})),
		events.Handle(scheduler.Event_UPDATE, events.HandlerFunc(func(e *scheduler.Event) error {
			state.metricsAPI.updatesReceived()
			statusUpdate(state, callOptions[:], e.GetUpdate().GetStatus())
			return nil
		})),
		events.Handle(scheduler.Event_ERROR, events.HandlerFunc(func(e *scheduler.Event) error {
			state.metricsAPI.errorsReceived()
			// it's recommended that we abort and re-try subscribing; setting
			// err here will cause the event loop to terminate and the connection
			// will be reset.
			return fmt.Errorf("ERROR: " + e.GetError().GetMessage())
		})),
		events.Handle(scheduler.Event_SUBSCRIBED, events.HandlerFunc(func(e *scheduler.Event) (err error) {
			state.metricsAPI.subscribedReceived()
			log.Println("received a SUBSCRIBED event")
			if state.frameworkID == "" {
				state.frameworkID = e.GetSubscribed().GetFrameworkID().GetValue()
				if state.frameworkID == "" {
					// sanity check
					err = errors.New("mesos gave us an empty frameworkID")
				} else {
					callOptions = append(callOptions, calls.Framework(state.frameworkID))
				}
			}
			return
		})),
	)
} // buildEventHandler

func failure(eid *mesos.ExecutorID, aid *mesos.AgentID, stat *int32) {
	if eid != nil {
		// executor failed..
		msg := "executor '" + eid.Value + "' terminated"
		if aid != nil {
			msg += "on agent '" + aid.Value + "'"
		}
		if stat != nil {
			msg += ", with status=" + strconv.Itoa(int(*stat))
		}
		log.Println(msg)
	} else if aid != nil {
		// agent failed..
		log.Println("agent '" + aid.Value + "' terminated")
	}
}

func refuseSecondsWithJitter(d time.Duration) scheduler.CallOpt {
	return calls.Filters(func(f *mesos.Filters) {
		s := time.Duration(rand.Int63n(int64(d))).Seconds()
		f.RefuseSeconds = &s
	})
}

func resourceOffers(state *internalState, callOptions scheduler.CallOptions, offers []mesos.Offer) {
	callOptions = append(callOptions, refuseSecondsWithJitter(state.config.maxRefuseSeconds))
	tasksLaunchedThisCycle := 0
	offersDeclined := 0
	for i := range offers {
		var (
			remaining = mesos.Resources(offers[i].Resources)
			tasks     = []mesos.TaskInfo{}
		)

		if state.config.verbose {
			log.Println("received offer id '" + offers[i].ID.Value + "' with resources " + remaining.String())
		}

		var wantsExecutorResources mesos.Resources
		if len(offers[i].ExecutorIDs) == 0 {
			wantsExecutorResources = mesos.Resources(state.executor.Resources)
		}

		flattened := remaining.Flatten()

		// avoid the expense of computing these if we can...
		if state.config.summaryMetrics && state.config.resourceTypeMetrics {
			for name, restype := range flattened.Types() {
				if restype == mesos.SCALAR {
					sum := flattened.SumScalars(mesos.NamedResources(name))
					state.metricsAPI.offeredResources(sum.GetValue(), name)
				}
			}
		}

		taskWantsResources := state.wantsTaskResources.Plus(wantsExecutorResources...)
		for state.tasksLaunched < state.totalTasks && flattened.ContainsAll(taskWantsResources) {
			state.tasksLaunched++
			taskID := state.tasksLaunched

			if state.config.verbose {
				log.Println("launching task " + strconv.Itoa(taskID) + " using offer " + offers[i].ID.Value)
			}

			task := mesos.TaskInfo{TaskID: mesos.TaskID{Value: strconv.Itoa(taskID)}}
			task.Name = "Task " + task.TaskID.Value
			task.AgentID = offers[i].AgentID
			task.Executor = state.executor
			task.Resources = remaining.Find(state.wantsTaskResources.Flatten(mesos.Role(state.role).Assign()))

			remaining.Subtract(task.Resources...)
			tasks = append(tasks, task)

			flattened = remaining.Flatten()
		}

		// build Accept call to launch all of the tasks we've assembled
		accept := calls.Accept(
			calls.OfferWithOperations(
				offers[i].ID,
				calls.OpLaunch(tasks...),
			),
		).With(callOptions...)

		// send Accept call to mesos
		err := state.cli.CallNoData(accept)
		if err != nil {
			state.metricsAPI.apiErrorCount("accept")
			log.Printf("failed to launch tasks: %+v", err)
		} else {
			if n := len(tasks); n > 0 {
				tasksLaunchedThisCycle += n
			} else {
				offersDeclined++
			}
		}
	}
	state.metricsAPI.offersDeclined.Int(offersDeclined)
	state.metricsAPI.tasksLaunched.Int(tasksLaunchedThisCycle)
	if state.config.summaryMetrics {
		state.metricsAPI.launchesPerOfferCycle(float64(tasksLaunchedThisCycle))
	}
}

func statusUpdate(state *internalState, callOptions scheduler.CallOptions, s mesos.TaskStatus) {
	msg := "Task " + s.TaskID.Value + " is in state " + s.GetState().String()
	if m := s.GetMessage(); m != "" {
		msg += " with message '" + m + "'"
	}
	if state.config.verbose {
		log.Println(msg)
	}

	if uuid := s.GetUUID(); len(uuid) > 0 {
		ack := calls.Acknowledge(
			s.GetAgentID().GetValue(),
			s.TaskID.Value,
			uuid,
		).With(callOptions...)

		// send Ack call to mesos
		err := state.cli.CallNoData(ack)
		if err != nil {
			state.metricsAPI.apiErrorCount("ack")
			log.Printf("failed to ack status update for task: %+v", err)
			return
		}
	}
	switch st := s.GetState(); st {
	case mesos.TASK_FINISHED:
		state.tasksFinished++
		state.metricsAPI.tasksFinished()

	case mesos.TASK_LOST, mesos.TASK_KILLED, mesos.TASK_FAILED, mesos.TASK_ERROR:
		state.err = errors.New("Exiting because task " + s.GetTaskID().Value +
			" is in an unexpected state " + st.String() +
			" with reason " + s.GetReason().String() +
			" from source " + s.GetSource().String() +
			" with message '" + s.GetMessage() + "'")
		state.done = true
		return
	}

	if state.tasksFinished == state.totalTasks {
		log.Println("mission accomplished, terminating")
		state.done = true
	} else {
		tryReviveOffers(state, callOptions)
	}
}

func tryReviveOffers(state *internalState, callOptions scheduler.CallOptions) {
	// limit the rate at which we request offer revival
	select {
	case <-state.reviveTokens:
		// not done yet, revive offers!
		state.metricsAPI.reviveCount()
		err := state.cli.CallNoData(calls.Revive().With(callOptions...))
		if err != nil {
			state.metricsAPI.apiErrorCount("revive")
			log.Printf("failed to revive offers: %+v", err)
			return
		}
	default:
		// noop
	}
}
