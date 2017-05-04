// msh is a minimal mesos v1 scheduler; it executes a shell command on a mesos agent.
package main

// Usage: msh {...command line args...}
//
// For example:
//    msh -master 10.2.0.5:5050 -- ls -laF /tmp
//
// TODO: -gpu=1 to enable GPU_RESOURCES caps and request 1 gpu
//

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/extras/latch"
	"github.com/mesos/mesos-go/api/v1/lib/extras/offers"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"
	"github.com/mesos/mesos-go/api/v1/lib/extras/store"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
)

const (
	RFC3339a = "20060102T150405Z0700"
)

var (
	FrameworkName = "msh"
	TaskName      = "msh"
	MesosMaster   = "127.0.0.1:5050"
	User          = "root"
	Role          = mesos.RoleName("*")
	CPUs          = float64(0.010)
	Memory        = float64(64)

	frameworkIDStore store.Singleton
	shouldDecline    bool
	refuseSeconds    = calls.RefuseSeconds(8 * time.Hour)
	stop             func()
	exitCode         int
	wantsResources   mesos.Resources
	taskPrototype    mesos.TaskInfo
)

func init() {
	flag.StringVar(&FrameworkName, "framework_name", FrameworkName, "Name of the framework")
	flag.StringVar(&TaskName, "task_name", TaskName, "Name of the msh task")
	flag.StringVar(&MesosMaster, "master", MesosMaster, "IP:port of the mesos master")
	flag.StringVar(&User, "user", User, "OS user that owns the launched task")
	flag.Float64Var(&CPUs, "cpus", CPUs, "CPU resources to allocate for the remote command")
	flag.Float64Var(&Memory, "memory", Memory, "Memory resources to allocate for the remote command")

	frameworkIDStore = store.NewInMemorySingleton()
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 { // msh by itself prints usage
		flag.Usage()
		os.Exit(1)
	}

	wantsResources = mesos.Resources{
		*mesos.CPUs(CPUs).Resource,
		*mesos.Memory(Memory).Resource,
	}
	taskPrototype = mesos.TaskInfo{
		Name: TaskName,
		Command: &mesos.CommandInfo{
			Value: proto.String(args[0]),
			Shell: proto.Bool(false),
		},
	}
	if len(args) > 1 {
		taskPrototype.Command.Arguments = args[1:]
	}
	err := controller.New().Run(buildControllerConfig(User))
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(exitCode)
}

func buildControllerConfig(user string) controller.Config {
	var (
		done   = new(latch.L).Reset()
		caller = calls.Decorators{
			calls.SubscribedCaller(frameworkIDStore.Get),
		}.Apply(buildClient())
	)
	stop = done.Close
	return controller.Config{
		Context: &controller.ContextAdapter{
			DoneFunc:        done.Closed,
			FrameworkIDFunc: frameworkIDStore.Get,
			ErrorFunc: func(err error) {
				defer stop()
				if err != nil {
					// don't overwrite an existing error code
					if exitCode == 0 {
						exitCode = 10
					}
					if err != io.EOF {
						log.Printf("%#v", err)
					}
					return
				}
				log.Println("disconnected")
			},
		},
		Framework: &mesos.FrameworkInfo{User: user, Name: FrameworkName, Role: (*string)(&Role)},
		Caller:    caller,
		Handler:   buildEventHandler(caller),
	}
}

func buildClient() calls.Caller {
	return httpsched.NewCaller(httpcli.New(
		httpcli.Endpoint(fmt.Sprintf("http://%s/api/v1/scheduler", MesosMaster)),
	))
}

func buildEventHandler(caller calls.Caller) events.Handler {
	ack := events.AcknowledgeUpdates(func() calls.Caller { return caller })
	return events.NewMux(
		events.DefaultHandler(events.HandlerFunc(controller.DefaultHandler)),
		events.MapFuncs(map[scheduler.Event_Type]events.HandlerFunc{
			scheduler.Event_OFFERS: func(e *scheduler.Event) error {
				return resourceOffers(caller, e.GetOffers().GetOffers())
			},
			scheduler.Event_UPDATE: func(e *scheduler.Event) error {
				err := ack.HandleEvent(e)
				if err != nil {
					err = fmt.Errorf("failed to ack status update for task: %#v", err)
				}
				statusUpdate(e.GetUpdate().GetStatus())
				return err
			},
			scheduler.Event_SUBSCRIBED: func(e *scheduler.Event) error {
				log.Println("received a SUBSCRIBED event")
				fid := e.GetSubscribed().GetFrameworkID().GetValue()
				if fid == "" {
					// sanity check, should **never** happen
					return fmt.Errorf("mesos gave us an empty frameworkID")
				}
				if current := frameworkIDStore.Get(); current != fid {
					err := frameworkIDStore.Set(fid)
					if err != nil {
						return err
					}
					log.Println("FrameworkID", fid)
				}
				return nil
			},
		}),
	)
}

func resourceOffers(caller calls.Caller, off []mesos.Offer) error {
	if shouldDecline {
		return calls.CallNoData(caller, calls.Suppress())
	}
	var (
		index = offers.NewIndex(off, nil)
		match = index.Find(offers.ContainsResources(wantsResources))
	)
	if match != nil {
		task := taskPrototype
		task.TaskID = mesos.TaskID{Value: time.Now().Format(RFC3339a)}
		task.AgentID = match.AgentID
		task.Resources = mesos.Resources(match.Resources).Find(wantsResources.Flatten(Role.Assign()))

		if err := calls.CallNoData(caller, calls.Accept(
			calls.OfferOperations{calls.OpLaunch(task)}.WithOffers(match.ID),
		)); err != nil {
			return err
		}

		shouldDecline = true // safeguard and suppress future offers
		if err := calls.CallNoData(caller, calls.Suppress()); err != nil {
			return err
		}
	} else {
		// insufficient offers
		log.Println("rejected insufficient offers")
	}
	// decline all but the possible match
	delete(index, match.GetID())
	return calls.CallNoData(caller, calls.Decline(index.IDs()...).With(refuseSeconds))
}

func statusUpdate(s mesos.TaskStatus) {
	switch st := s.GetState(); st {
	case mesos.TASK_FINISHED, mesos.TASK_RUNNING, mesos.TASK_STAGING, mesos.TASK_STARTING:
		log.Println("status update", st)
		if st != mesos.TASK_FINISHED {
			return
		}
	case mesos.TASK_LOST, mesos.TASK_KILLED, mesos.TASK_FAILED, mesos.TASK_ERROR:
		log.Println("Exiting because task " + s.GetTaskID().Value +
			" is in an unexpected state " + st.String() +
			" with reason " + s.GetReason().String() +
			" from source " + s.GetSource().String() +
			" with message '" + s.GetMessage() + "'")
		exitCode = 3
	default:
		log.Println("unexpected task state, aborting", st)
		exitCode = 4
	}
	stop()
}
