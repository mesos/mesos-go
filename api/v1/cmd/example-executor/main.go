package main

import (
	"errors"
	"io"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/executor"
	"github.com/mesos/mesos-go/api/v1/lib/executor/calls"
	"github.com/mesos/mesos-go/api/v1/lib/executor/config"
	"github.com/mesos/mesos-go/api/v1/lib/executor/events"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/pborman/uuid"
)

const (
	apiPath     = "/api/v1/executor"
	httpTimeout = 10 * time.Second
)

var errMustAbort = errors.New("received abort signal from mesos, will attempt to re-subscribe")

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatal("failed to load configuration: " + err.Error())
	}
	log.Printf("configuration loaded: %+v", cfg)
	run(cfg)
	os.Exit(0)
}

func run(cfg config.Config) {
	var (
		apiURL = url.URL{
			Scheme: "http", // TODO(jdef) make this configurable
			Host:   cfg.AgentEndpoint,
			Path:   apiPath,
		}
		state = &internalState{
			cli: httpcli.New(
				httpcli.Endpoint(apiURL.String()),
				httpcli.Codec(&encoding.ProtobufCodec),
				httpcli.Do(httpcli.With(httpcli.Timeout(httpTimeout))),
			),
			callOptions: executor.CallOptions{
				calls.Framework(cfg.FrameworkID),
				calls.Executor(cfg.ExecutorID),
			},
			unackedTasks:   make(map[mesos.TaskID]mesos.TaskInfo),
			unackedUpdates: make(map[string]executor.Call_Update),
			failedTasks:    make(map[mesos.TaskID]mesos.TaskStatus),
		}
		subscribe       = calls.Subscribe(nil, nil).With(state.callOptions...)
		shouldReconnect = backoff.Notifier(1*time.Second, cfg.SubscriptionBackoffMax*4/3, nil)
		disconnected    = time.Now()
		handler         = buildEventHandler(state)
	)
	for {
		subscribe = subscribe.With(
			unacknowledgedTasks(state),
			unacknowledgedUpdates(state),
		)
		func() {
			resp, err := state.cli.Do(subscribe, httpcli.Close(true))
			if resp != nil {
				defer resp.Close()
			}
			if err == nil {
				// we're officially connected, start decoding events
				err = eventLoop(state, resp.Decoder(), handler)
				disconnected = time.Now()
			}
			if err != nil && err != io.EOF {
				log.Println(err)
			} else {
				log.Println("disconnected")
			}
		}()
		if state.shouldQuit {
			log.Println("gracefully shutting down because we were told to")
			return
		}
		if !cfg.Checkpoint {
			log.Println("gracefully exiting because framework checkpointing is NOT enabled")
			return
		}
		if time.Now().Sub(disconnected) > cfg.RecoveryTimeout {
			log.Printf("failed to re-establish subscription with agent within %v, aborting", cfg.RecoveryTimeout)
			return
		}
		<-shouldReconnect // wait for some amount of time before retrying subscription
	}
}

// unacknowledgedTasks is a functional option that sets the value of the UnacknowledgedTasks
// field of a Subscribe call.
func unacknowledgedTasks(state *internalState) executor.CallOpt {
	return func(call *executor.Call) {
		if n := len(state.unackedTasks); n > 0 {
			unackedTasks := make([]mesos.TaskInfo, 0, n)
			for k := range state.unackedTasks {
				unackedTasks = append(unackedTasks, state.unackedTasks[k])
			}
			call.Subscribe.UnacknowledgedTasks = unackedTasks
		} else {
			call.Subscribe.UnacknowledgedTasks = nil
		}
	}
}

// unacknowledgedUpdates is a functional option that sets the value of the UnacknowledgedUpdates
// field of a Subscribe call.
func unacknowledgedUpdates(state *internalState) executor.CallOpt {
	return func(call *executor.Call) {
		if n := len(state.unackedUpdates); n > 0 {
			unackedUpdates := make([]executor.Call_Update, 0, n)
			for k := range state.unackedUpdates {
				unackedUpdates = append(unackedUpdates, state.unackedUpdates[k])
			}
			call.Subscribe.UnacknowledgedUpdates = unackedUpdates
		} else {
			call.Subscribe.UnacknowledgedUpdates = nil
		}
	}
}

func eventLoop(state *internalState, decoder encoding.Decoder, h events.Handler) (err error) {
	for err == nil && !state.shouldQuit {
		// housekeeping
		sendFailedTasks(state)

		var e executor.Event
		if err = decoder.Invoke(&e); err == nil {
			err = h.HandleEvent(&e)
		}
	}
	return err
}

func buildEventHandler(state *internalState) events.Handler {
	return events.NewMux(
		events.Handle(executor.Event_SUBSCRIBED, events.HandlerFunc(func(e *executor.Event) error {
			log.Println("SUBSCRIBED")
			state.framework = e.Subscribed.FrameworkInfo
			state.executor = e.Subscribed.ExecutorInfo
			state.agent = e.Subscribed.AgentInfo
			return nil
		})),
		events.Handle(executor.Event_LAUNCH, events.HandlerFunc(func(e *executor.Event) error {
			launch(state, e.Launch.Task)
			return nil
		})),
		events.Handle(executor.Event_KILL, events.HandlerFunc(func(e *executor.Event) error {
			log.Println("warning: KILL not implemented")
			return nil
		})),
		events.Handle(executor.Event_ACKNOWLEDGED, events.HandlerFunc(func(e *executor.Event) error {
			delete(state.unackedTasks, e.Acknowledged.TaskID)
			delete(state.unackedUpdates, string(e.Acknowledged.UUID))
			return nil
		})),
		events.Handle(executor.Event_MESSAGE, events.HandlerFunc(func(e *executor.Event) error {
			log.Printf("MESSAGE: received %d bytes of message data", len(e.Message.Data))
			return nil
		})),
		events.Handle(executor.Event_SHUTDOWN, events.HandlerFunc(func(e *executor.Event) error {
			log.Println("SHUTDOWN received")
			state.shouldQuit = true
			return nil
		})),
		events.Handle(executor.Event_ERROR, events.HandlerFunc(func(e *executor.Event) error {
			log.Println("ERROR received")
			return errMustAbort
		})),
	)
}

func sendFailedTasks(state *internalState) {
	for taskID, status := range state.failedTasks {
		updateErr := update(state, status)
		if updateErr != nil {
			log.Printf("failed to send status update for task %s: %+v", taskID.Value, updateErr)
		} else {
			delete(state.failedTasks, taskID)
		}
	}
}

func launch(state *internalState, task mesos.TaskInfo) {
	state.unackedTasks[task.TaskID] = task

	// send RUNNING
	status := newStatus(state, task.TaskID)
	status.State = mesos.TASK_RUNNING.Enum()
	err := update(state, status)
	if err != nil {
		log.Printf("failed to send TASK_RUNNING for task %s: %+v", task.TaskID.Value, err)
		status.State = mesos.TASK_FAILED.Enum()
		status.Message = protoString(err.Error())
		state.failedTasks[task.TaskID] = status
		return
	}

	// send FINISHED
	status = newStatus(state, task.TaskID)
	status.State = mesos.TASK_FINISHED.Enum()
	err = update(state, status)
	if err != nil {
		log.Printf("failed to send TASK_FINISHED for task %s: %+v", task.TaskID.Value, err)
		status.State = mesos.TASK_FAILED.Enum()
		status.Message = protoString(err.Error())
		state.failedTasks[task.TaskID] = status
	}
}

// helper func to package strings up nicely for protobuf
// NOTE(jdef): if we need any more proto funcs like this, just import the
// proto package and use those.
func protoString(s string) *string { return &s }

func update(state *internalState, status mesos.TaskStatus) error {
	upd := calls.Update(status).With(state.callOptions...)
	resp, err := state.cli.Do(upd)
	if resp != nil {
		resp.Close()
	}
	if err != nil {
		log.Printf("failed to send update: %+v", err)
		debugJSON(upd)
	} else {
		state.unackedUpdates[string(status.UUID)] = *upd.Update
	}
	return err
}

func newStatus(state *internalState, id mesos.TaskID) mesos.TaskStatus {
	return mesos.TaskStatus{
		TaskID:     id,
		Source:     mesos.SOURCE_EXECUTOR.Enum(),
		ExecutorID: &state.executor.ExecutorID,
		UUID:       []byte(uuid.NewRandom()),
	}
}

type internalState struct {
	callOptions    executor.CallOptions
	cli            *httpcli.Client
	cfg            config.Config
	framework      mesos.FrameworkInfo
	executor       mesos.ExecutorInfo
	agent          mesos.AgentInfo
	unackedTasks   map[mesos.TaskID]mesos.TaskInfo
	unackedUpdates map[string]executor.Call_Update
	failedTasks    map[mesos.TaskID]mesos.TaskStatus // send updates for these as we can
	shouldQuit     bool
}
