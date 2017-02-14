package app

import (
	"errors"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	xmetrics "github.com/mesos/mesos-go/api/v1/lib/extras/metrics"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
)

var (
	RegistrationMinBackoff = 1 * time.Second
	RegistrationMaxBackoff = 15 * time.Second
)

func Run(cfg Config) error {
	log.Printf("scheduler running with configuration: %+v", cfg)
	shutdown := make(chan struct{})
	defer close(shutdown)

	state, err := newInternalState(cfg)
	if err != nil {
		return err
	}

	// TODO(jdef) how to track/handle timeout errors that occur for SUBSCRIBE calls? we should
	// probably tolerate X number of subsequent subscribe failures before bailing. we'll need
	// to track the lastCallAttempted along with subsequentSubscribeTimeouts.
	err = controller.New().Run(buildControllerConfig(state, shutdown))
	if state.err != nil {
		err = state.err
	}
	return err
}

func buildControllerConfig(state *internalState, shutdown <-chan struct{}) controller.Config {
	controlContext := &controller.ContextAdapter{
		DoneFunc:        func() bool { return state.done },
		FrameworkIDFunc: func() string { return state.frameworkID },
		ErrorFunc: func(err error) {
			if err != nil && err != io.EOF {
				log.Println(err)
			} else {
				log.Println("disconnected")
			}
		},
	}

	state.cli = calls.Decorators{
		callMetrics(state.metricsAPI, time.Now, state.config.summaryMetrics),
		logCalls(map[scheduler.Call_Type]string{scheduler.Call_SUBSCRIBE: "connecting..."}),
	}.Apply(state.cli)

	return controller.Config{
		Context:            controlContext,
		Framework:          buildFrameworkInfo(state.config),
		Caller:             state.cli,
		RegistrationTokens: backoff.Notifier(RegistrationMinBackoff, RegistrationMaxBackoff, shutdown),

		Handler: events.Decorators{
			eventMetrics(state.metricsAPI, time.Now, state.config.summaryMetrics),
			events.Decorator(logAllEvents).If(state.config.verbose),
		}.Apply(buildEventHandler(state)),
	}
}

// buildEventHandler generates and returns a handler to process events received from the subscription.
func buildEventHandler(state *internalState) events.Handler {
	// TODO(jdef) would be nice to merge this ack handler with the status update handler below; need to
	// figure out appropriate error propagation among chained handlers.
	ack := events.AcknowledgeUpdates(func() calls.Caller { return state.cli })
	return events.NewMux(
		events.DefaultHandler(events.HandlerFunc(controller.DefaultHandler)),
		events.MapFuncs(map[scheduler.Event_Type]events.HandlerFunc{
			scheduler.Event_FAILURE: func(e *scheduler.Event) error {
				log.Println("received a FAILURE event")
				f := e.GetFailure()
				failure(f.ExecutorID, f.AgentID, f.Status)
				return nil
			},
			scheduler.Event_OFFERS: func(e *scheduler.Event) error {
				if state.config.verbose {
					log.Println("received an OFFERS event")
				}
				offers := e.GetOffers().GetOffers()
				state.metricsAPI.offersReceived.Int(len(offers))
				resourceOffers(state, offers)
				return nil
			},
			scheduler.Event_UPDATE: func(e *scheduler.Event) error {
				if err := ack.HandleEvent(e); err != nil {
					log.Printf("failed to ack status update for task: %+v", err)
					// TODO(jdef) we don't return the error because that would cause the subscription
					// to terminate; is that the right thing to do?
				}
				statusUpdate(state, e.GetUpdate().GetStatus())
				return nil
			},
			scheduler.Event_SUBSCRIBED: func(e *scheduler.Event) (err error) {
				log.Println("received a SUBSCRIBED event")
				if state.frameworkID == "" {
					state.frameworkID = e.GetSubscribed().GetFrameworkID().GetValue()
					if state.frameworkID == "" {
						// sanity check
						err = errors.New("mesos gave us an empty frameworkID")
					} else {
						// automatically set the frameworkID for all outgoing calls
						state.cli = calls.FrameworkCaller(state.frameworkID).Apply(state.cli)
					}
				}
				return
			},
		}),
	)
}

func failure(eid *mesos.ExecutorID, aid *mesos.AgentID, stat *int32) {
	if eid != nil {
		// executor failed..
		msg := "executor '" + eid.Value + "' terminated"
		if aid != nil {
			msg += " on agent '" + aid.Value + "'"
		}
		if stat != nil {
			msg += " with status=" + strconv.Itoa(int(*stat))
		}
		log.Println(msg)
	} else if aid != nil {
		// agent failed..
		log.Println("agent '" + aid.Value + "' terminated")
	}
}

func resourceOffers(state *internalState, offers []mesos.Offer) {
	callOption := calls.RefuseSecondsWithJitter(state.random, state.config.maxRefuseSeconds)
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

			task := mesos.TaskInfo{
				TaskID:    mesos.TaskID{Value: strconv.Itoa(taskID)},
				AgentID:   offers[i].AgentID,
				Executor:  state.executor,
				Resources: remaining.Find(state.wantsTaskResources.Flatten(mesos.RoleName(state.role).Assign())),
			}
			task.Name = "Task " + task.TaskID.Value

			remaining.Subtract(task.Resources...)
			tasks = append(tasks, task)

			flattened = remaining.Flatten()
		}

		// build Accept call to launch all of the tasks we've assembled
		accept := calls.Accept(
			calls.OfferOperations{calls.OpLaunch(tasks...)}.WithOffers(offers[i].ID),
		).With(callOption)

		// send Accept call to mesos
		err := calls.CallNoData(state.cli, accept)
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

func statusUpdate(state *internalState, s mesos.TaskStatus) {
	if state.config.verbose {
		msg := "Task " + s.TaskID.Value + " is in state " + s.GetState().String()
		if m := s.GetMessage(); m != "" {
			msg += " with message '" + m + "'"
		}
		log.Println(msg)
	}

	switch st := s.GetState(); st {
	case mesos.TASK_FINISHED:
		state.tasksFinished++
		state.metricsAPI.tasksFinished()

		if state.tasksFinished == state.totalTasks {
			log.Println("mission accomplished, terminating")
			state.done = true
		} else {
			tryReviveOffers(state)
		}

	case mesos.TASK_LOST, mesos.TASK_KILLED, mesos.TASK_FAILED, mesos.TASK_ERROR:
		state.err = errors.New("Exiting because task " + s.GetTaskID().Value +
			" is in an unexpected state " + st.String() +
			" with reason " + s.GetReason().String() +
			" from source " + s.GetSource().String() +
			" with message '" + s.GetMessage() + "'")
		state.done = true
	}
}

func tryReviveOffers(state *internalState) {
	// limit the rate at which we request offer revival
	select {
	case <-state.reviveTokens:
		// not done yet, revive offers!
		err := calls.CallNoData(state.cli, calls.Revive())
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
func eventMetrics(metricsAPI *metricsAPI, clock func() time.Time, timingMetrics bool) events.Decorator {
	timed := metricsAPI.eventReceivedLatency
	if !timingMetrics {
		timed = nil
	}
	harness := xmetrics.NewHarness(metricsAPI.eventReceivedCount, metricsAPI.eventErrorCount, timed, clock)
	return events.Metrics(harness)
}

// callMetrics logs metrics for every outgoing Mesos call
func callMetrics(metricsAPI *metricsAPI, clock func() time.Time, timingMetrics bool) calls.Decorator {
	timed := metricsAPI.callLatency
	if !timingMetrics {
		timed = nil
	}
	harness := xmetrics.NewHarness(metricsAPI.callCount, metricsAPI.callErrorCount, timed, clock)
	return calls.CallerMetrics(harness)
}

// logCalls logs a specific message string when a particular call-type is observed
func logCalls(messages map[scheduler.Call_Type]string) calls.Decorator {
	return func(caller calls.Caller) calls.Caller {
		return calls.CallerFunc(func(c *scheduler.Call) (mesos.Response, error) {
			if message, ok := messages[c.GetType()]; ok {
				log.Println(message)
			}
			return caller.Call(c)
		})
	}
}
