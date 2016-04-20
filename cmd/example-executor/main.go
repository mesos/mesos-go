package main

import (
	"errors"
	"io"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/backoff"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/executor"
	"github.com/mesos/mesos-go/executor/calls"
	"github.com/mesos/mesos-go/executor/config"
	"github.com/mesos/mesos-go/httpcli"
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
				httpcli.URL(apiURL.String()),
				httpcli.Codec(&encoding.ProtobufCodec),
				httpcli.Do(httpcli.With(httpcli.Timeout(httpTimeout))),
				httpcli.MaxRedirects(0), // no redirects for agent connections; there is only ever one
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
				err = eventLoop(state, resp.Decoder())
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

func eventLoop(state *internalState, decoder encoding.Decoder) (err error) {
	for err == nil {
		var e executor.Event
		if err = decoder.Invoke(&e); err != nil {
			continue
		}
		switch e.GetType() {
		case executor.Event_SUBSCRIBED:
			log.Println("SUBSCRIBED")
			state.framework = e.Subscribed.FrameworkInfo
			state.executor = e.Subscribed.ExecutorInfo
			state.agent = e.Subscribed.AgentInfo

		case executor.Event_LAUNCH:
			launch(state, e.Launch.Task)

		case executor.Event_KILL:
			log.Println("warning: KILL not implemented")

		case executor.Event_ACKNOWLEDGED:
			delete(state.unackedTasks, e.Acknowledged.TaskID)
			delete(state.unackedUpdates, string(e.Acknowledged.UUID))

		case executor.Event_MESSAGE:
			log.Printf("MESSAGE: received %d bytes of message data", len(e.Message.Data))

		case executor.Event_SHUTDOWN:
			log.Println("SHUTDOWN received")
			state.shouldQuit = true
			return nil

		case executor.Event_ERROR:
			log.Println("ERROR received")
			err = errMustAbort
		}

		// housekeeping
		if err == nil {
			sendFailedTasks(state)
		}
	}
	return err
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
