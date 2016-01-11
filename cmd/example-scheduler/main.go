package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli"
	"github.com/mesos/mesos-go/scheduler"
)

func main() {
	cfg := config{
		user:       "foobar",
		name:       "example",
		url:        "http://:5050/api/v1/scheduler",
		codec:      codec{Codec: &encoding.ProtobufCodec},
		timeout:    time.Second,
		checkpoint: true,
	}

	fs := flag.NewFlagSet("example-scheduler", flag.ExitOnError)
	fs.StringVar(&cfg.user, "user", cfg.user, "Framework user to register with the Mesos master")
	fs.StringVar(&cfg.name, "name", cfg.name, "Framework name to register with the Mesos master")
	fs.Var(&cfg.codec, "codec", "Codec to encode/decode scheduler API communications [protobuf, json]")
	fs.StringVar(&cfg.url, "url", cfg.url, "Mesos scheduler API URL")
	fs.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "Mesos scheduler API connection timeout")
	fs.BoolVar(&cfg.checkpoint, "checkpoint", cfg.checkpoint, "Enable/disable framework checkpointing")
	fs.StringVar(&cfg.principal, "principal", cfg.principal, "Framework principal with which to authenticate")
	fs.StringVar(&cfg.hostname, "hostname", cfg.hostname, "Framework hostname that is advertised to the master")
	fs.Var(&cfg.labels, "label", "Framework label, may be specified multiple times")
	fs.Parse(os.Args[1:])

	if err := run(&cfg); err != nil {
		log.Fatal(err)
	}
}

type internalState struct {
	tasksLaunched int
	totalTasks    int
	frameworkID   string
	role          string
	executor      *mesos.ExecutorInfo
}

func run(cfg *config) error {
	cli := httpcli.New(
		httpcli.URL(cfg.url),
		httpcli.Codec(cfg.codec.Codec),
		httpcli.Do(httpcli.With(httpcli.Timeout(cfg.timeout))),
	)

	frameworkInfo := &mesos.FrameworkInfo{
		User:       cfg.user,
		Name:       cfg.name,
		Checkpoint: &cfg.checkpoint,
	}
	if cfg.principal != "" {
		frameworkInfo.Principal = &cfg.principal
	}
	if cfg.hostname != "" {
		frameworkInfo.Hostname = &cfg.hostname
	}
	if len(cfg.labels) > 0 {
		log.Println("using labels:", cfg.labels)
		frameworkInfo.Labels = &mesos.Labels{Labels: cfg.labels}
	}
	subscribe := scheduler.SubscribeCall(true, frameworkInfo)
	registrationTokens := backoffBucket(1*time.Second, 15*time.Second, nil)
	var state internalState
	for {
		events, conn, err := cli.Do(subscribe, httpcli.Close(true))
		if err == nil {
			err = eventLoop(&state, events, conn)
		}
		if err != nil {
			log.Println(err)
		} else {
			log.Println("disconnected")
		}
		if state.frameworkID != "" {
			subscribe.Subscribe.FrameworkInfo.ID = &mesos.FrameworkID{Value: state.frameworkID}
		}
		<-registrationTokens
		log.Println("reconnecting..")
	}
}

// returns the framework ID received by mesos (if any); callers should check for a
// framework ID regardless of whether error != nil.
func eventLoop(state *internalState, events encoding.Decoder, conn io.Closer) (err error) {
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	state.frameworkID = ""
	callOptions := []scheduler.CallOpt{} // should be applied to every outgoing call

	for err == nil {
		var e scheduler.Event
		if err = events.Decode(&e); err != nil {
			if err == io.EOF {
				err = nil
			}
			continue
		}

		switch e.GetType().Enum() {
		case scheduler.Event_FAILURE.Enum():
			log.Println("received a FAILURE event")
			if eid := e.GetFailure().GetExecutorID(); eid != nil {
				// executor failed..
				msg := "executor '" + eid.Value + "' terminated"
				if e.Failure.AgentID != nil {
					msg += "on agent '" + e.Failure.AgentID.Value + "'"
				}
				if e.Failure.Status != nil {
					msg += ", with status=" + strconv.Itoa(int(*e.Failure.Status))
				}
				log.Println(msg)
			} else if e.GetFailure().GetAgentID() != nil {
				// agent failed..
				log.Println("agent '" + e.Failure.AgentID.Value + "' terminated")
			}

		case scheduler.Event_OFFERS.Enum():
			log.Println("received an OFFERS event")
			resourceOffers(state, e.GetOffers().GetOffers())

		case scheduler.Event_UPDATE.Enum():
			statusUpdate(e.GetUpdate().GetStatus())

		case scheduler.Event_ERROR.Enum():
			// it's recommended that we abort and re-try subscribing; setting
			// err here will cause the event loop to terminate and the connection
			// will be reset.
			err = fmt.Errorf("ERROR: " + e.GetError().GetMessage())

		case scheduler.Event_SUBSCRIBED.Enum():
			log.Println("received a SUBSCRIBED event")
			if state.frameworkID == "" {
				state.frameworkID = e.GetSubscribed().GetFrameworkID().GetValue()
				if state.frameworkID == "" {
					// sanity check
					panic("mesos gave us an empty frameworkID")
				}
				callOptions = append(callOptions, scheduler.Framework(state.frameworkID))
			}
			// else, ignore subsequently received events like this on the same connection
		default:
			// handle unknown event
		}

		log.Printf("%+v\n", e)
	}
	return err
}

const (
	CPUS_PER_TASK = 1
	MEM_PER_TASK  = 128
)

var wantsTaskResources = mesos.Resources{
	*mesos.BuildResource().Name("cpus").Scalar(CPUS_PER_TASK).Resource,
	*mesos.BuildResource().Name("mem").Scalar(MEM_PER_TASK).Resource,
}

func resourceOffers(state *internalState, offers []mesos.Offer) {
	for i := range offers {
		log.Println("received offer id '" + offers[i].ID.Value + "'")
		var (
			remaining = mesos.Resources(offers[i].Resources)
			tasks     = []mesos.TaskInfo{}
		)
		for state.tasksLaunched < state.totalTasks && remaining.Flatten("", nil).ContainsAll(wantsTaskResources) {
			state.tasksLaunched++
			taskID := state.tasksLaunched

			log.Println("launching task " + strconv.Itoa(taskID) + " using offer " + offers[i].ID.Value)

			task := mesos.TaskInfo{TaskID: mesos.TaskID{Value: strconv.Itoa(taskID)}}
			task.Name = "Task " + task.TaskID.Value
			task.AgentID = offers[i].AgentID
			task.Executor = state.executor
			task.Resources = remaining.Find(wantsTaskResources.Flatten(state.role, nil))

			remaining.Subtract(task.Resources...)
			tasks = append(tasks, task)
		}
	}
	// TODO .. send ACCEPT call to launch tasks
}

func statusUpdate(s mesos.TaskStatus) {
	// TODO..
}

type config struct {
	id         string
	user       string
	name       string
	url        string
	codec      codec
	timeout    time.Duration
	checkpoint bool
	principal  string
	hostname   string
	labels     Labels
}
