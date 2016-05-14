package app

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/backoff"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli/httpsched"
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
		handler            = events.Decorators{
			eventMetrics(state.metricsAPI, time.Now, state.config.summaryMetrics),
			events.Decorator(logAllEvents).If(state.config.verbose),
		}.Apply(buildEventHandler(state))
		decorateCaller = callMetrics(state.metricsAPI, time.Now, state.config.summaryMetrics)
	)
	state.cli = decorateCaller(state.cli)
	for !state.done {
		if frameworkInfo.GetFailoverTimeout() > 0 && state.frameworkID != "" {
			subscribe.Subscribe.FrameworkInfo.ID = &mesos.FrameworkID{Value: state.frameworkID}
		}
		<-registrationTokens
		log.Println("connecting..")
		resp, subscribedCaller, err := state.cli.Call(subscribe)
		processSubscription(state, handler, resp, decorateCaller(subscribedCaller), err)
	}
	return state.err
}

func processSubscription(
	state *internalState,
	handler events.Handler,
	resp mesos.Response,
	subscribedCaller httpsched.Caller,
	err error,
) {
	if resp != nil {
		defer resp.Close()
	}
	if err == nil {
		state.frameworkID = "" // we're newly (re?)subscribed, forget this
		if subscribedCaller != nil {
			// subscribedCaller is only good for the duration of the subscription
			oldCaller := state.cli
			state.cli = subscribedCaller
			defer func() { state.cli = oldCaller }()
		}
		err = eventLoop(state, resp.Decoder(), handler)
	}
	if err != nil && err != io.EOF {
		log.Println(err)
	} else {
		log.Println("disconnected")
	}
}

// eventLoop returns the framework ID received by mesos (if any); callers should check for a
// framework ID regardless of whether error != nil.
func eventLoop(state *internalState, eventDecoder encoding.Decoder, handler events.Handler) (err error) {
	for err == nil && !state.done {
		var e scheduler.Event
		if err = eventDecoder.Invoke(&e); err == nil {
			err = handler.HandleEvent(&e)
		}
	}
	return err
}

// buildEventHandler generates and returns a handler to process events received from the subscription.
func buildEventHandler(state *internalState) events.Handler {
	callOptions := scheduler.CallOptions{} // should be applied to every outgoing call
	return events.NewMux(
		events.Handle(scheduler.Event_FAILURE, events.HandlerFunc(func(e *scheduler.Event) error {
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
			resourceOffers(state, callOptions[:], offers)
			return nil
		})),
		events.Handle(scheduler.Event_UPDATE, events.HandlerFunc(func(e *scheduler.Event) error {
			statusUpdate(state, callOptions[:], e.GetUpdate().GetStatus())
			return nil
		})),
		events.Handle(scheduler.Event_ERROR, events.HandlerFunc(func(e *scheduler.Event) error {
			// it's recommended that we abort and re-try subscribing; setting
			// err here will cause the event loop to terminate and the connection
			// will be reset.
			return fmt.Errorf("ERROR: " + e.GetError().GetMessage())
		})),
		events.Handle(scheduler.Event_SUBSCRIBED, events.HandlerFunc(func(e *scheduler.Event) (err error) {
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
		err := httpsched.CallNoData(state.cli, accept)
		if err != nil {
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
		err := httpsched.CallNoData(state.cli, ack)
		if err != nil {
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
		err := httpsched.CallNoData(state.cli, calls.Revive().With(callOptions...))
		if err != nil {
			log.Printf("failed to revive offers: %+v", err)
			return
		}
	default:
		// noop
	}
}

// logAllEvents logs every observed event; this is somewhat expensive to do
func logAllEvents(h events.Handler) events.Handler {
	return events.HandlerFunc(func(e *scheduler.Event) error {
		log.Printf("%+v\n", *e)
		return h.HandleEvent(e)
	})
}

// eventMetrics logs metrics for every processed API event
func eventMetrics(metricsAPI *metricsAPI, clock func() time.Time, summaryMetrics bool) events.Decorator {
	timed := metricsAPI.eventReceivedLatency
	if !summaryMetrics {
		timed = nil
	}
	harness := newMetricsHarness(metricsAPI.eventReceivedCount, metricsAPI.eventErrorCount, timed, clock)
	return func(h events.Handler) events.Handler {
		return events.HandlerFunc(func(e *scheduler.Event) error {
			typename := strings.ToLower(e.GetType().String())
			return harness(func() error { return h.HandleEvent(e) }, typename)
		})
	}
}

func callMetrics(metricsAPI *metricsAPI, clock func() time.Time, summaryMetrics bool) httpsched.Decorator {
	timed := metricsAPI.callLatency
	if !summaryMetrics {
		timed = nil
	}
	harness := newMetricsHarness(metricsAPI.callCount, metricsAPI.callErrorCount, timed, clock)
	return func(caller httpsched.Caller) (metricsCaller httpsched.Caller) {
		if caller != nil {
			metricsCaller = &httpsched.CallerAdapter{
				CallFunc: func(c *scheduler.Call) (res mesos.Response, caller2 httpsched.Caller, err error) {
					typename := strings.ToLower(c.GetType().String())
					harness(func() error {
						res, caller2, err = caller.Call(c)
						return err // need to count these
					}, typename)
					return
				},
			}
		}
		return
	}
}
