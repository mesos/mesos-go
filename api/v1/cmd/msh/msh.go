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
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/eventrules"
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

	frameworkIDStore   store.Singleton
	declineAndSuppress bool
	refuseSeconds      = calls.RefuseSeconds(5 * time.Second)
	wantsResources     mesos.Resources
	taskPrototype      mesos.TaskInfo
)

func init() {
	flag.StringVar(&FrameworkName, "framework_name", FrameworkName, "Name of the framework")
	flag.StringVar(&TaskName, "task_name", TaskName, "Name of the msh task")
	flag.StringVar(&MesosMaster, "master", MesosMaster, "IP:port of the mesos master")
	flag.StringVar(&User, "user", User, "OS user that owns the launched task")
	flag.Float64Var(&CPUs, "cpus", CPUs, "CPU resources to allocate for the remote command")
	flag.Float64Var(&Memory, "memory", Memory, "Memory resources to allocate for the remote command")

	frameworkIDStore = store.DecorateSingleton(
		store.NewInMemorySingleton(),
		store.DoSet().AndThen(func(_ store.Setter, v string, _ error) error {
			log.Println("FrameworkID", v)
			return nil
		}))
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 { // msh by itself prints usage
		flag.Usage()
		os.Exit(1)
	}

	wantsResources = mesos.Resources{
		mesos.CPUs(CPUs).Resource,
		mesos.Memory(Memory).Resource,
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
	if err := run(); err != nil {
		if exitErr, ok := err.(ExitError); ok {
			if code := int(exitErr); code != 0 {
				log.Println(exitErr)
				os.Exit(code)
			}
			// else, code=0 indicates success, exit normally
		} else {
			panic(fmt.Sprintf("%#v", err))
		}
	}
}

func run() error {
	var (
		done   = latch.New()
		caller = calls.Decorators{
			calls.SubscribedCaller(store.GetIgnoreErrors(frameworkIDStore)),
		}.Apply(buildClient())
	)

	return controller.Run(
		&mesos.FrameworkInfo{User: User, Name: FrameworkName, Role: (*string)(&Role)},
		caller,
		controller.WithDone(done.Closed),
		controller.WithEventHandler(buildEventHandler(caller)),
		controller.WithFrameworkID(store.GetIgnoreErrors(frameworkIDStore)),
		controller.WithSubscriptionTerminated(func(err error) {
			defer done.Close()
			if err == io.EOF {
				log.Println("disconnected")
			}
		}),
	)
}

func buildClient() calls.Caller {
	return httpsched.NewCaller(httpcli.New(
		httpcli.Endpoint(fmt.Sprintf("http://%s/api/v1/scheduler", MesosMaster)),
	))
}

func buildEventHandler(caller calls.Caller) events.Handler {
	logger := controller.LogEvents()
	return controller.LiftErrors().Handle(events.HandlerSet{
		scheduler.Event_FAILURE:    logger,
		scheduler.Event_SUBSCRIBED: eventrules.Rules{logger, controller.TrackSubscription(frameworkIDStore, 0)},
		scheduler.Event_OFFERS:     maybeDeclineOffers(caller).AndThen().Handle(resourceOffers(caller)),
		scheduler.Event_UPDATE:     controller.AckStatusUpdates(caller).AndThen().HandleF(statusUpdate),
	})
}

func maybeDeclineOffers(caller calls.Caller) eventrules.Rule {
	return func(e *scheduler.Event, err error, chain eventrules.Chain) (*scheduler.Event, error) {
		if err != nil {
			return chain(e, err)
		}
		if e.GetType() != scheduler.Event_OFFERS || !declineAndSuppress {
			return chain(e, err)
		}
		off := offers.Slice(e.GetOffers().GetOffers())
		err = calls.CallNoData(caller, calls.Decline(off.IDs()...).With(refuseSeconds))
		if err == nil {
			// we shouldn't have received offers, maybe the prior suppress call failed?
			err = calls.CallNoData(caller, calls.Suppress())
		}
		return nil, err // drop
	}
}

func resourceOffers(caller calls.Caller) events.HandlerFunc {
	return func(e *scheduler.Event) (err error) {
		var (
			off   = e.GetOffers().GetOffers()
			index = offers.NewIndex(off, nil)
			match = index.Find(offers.ContainsResources(wantsResources))
		)
		if match != nil {
			task := taskPrototype
			task.TaskID = mesos.TaskID{Value: time.Now().Format(RFC3339a)}
			task.AgentID = match.AgentID
			task.Resources = mesos.Resources(match.Resources).Find(wantsResources.Flatten(Role.Assign()))

			err = calls.CallNoData(caller, calls.Accept(
				calls.OfferOperations{calls.OpLaunch(task)}.WithOffers(match.ID),
			))
			if err != nil {
				return
			}

			declineAndSuppress = true
		} else {
			log.Println("rejected insufficient offers")
		}
		// decline all but the possible match
		delete(index, match.GetID())
		err = calls.CallNoData(caller, calls.Decline(index.IDs()...).With(refuseSeconds))
		if err != nil {
			return
		}
		if declineAndSuppress {
			err = calls.CallNoData(caller, calls.Suppress())
		}
		return
	}
}

func statusUpdate(e *scheduler.Event) error {
	s := e.GetUpdate().GetStatus()
	switch st := s.GetState(); st {
	case mesos.TASK_FINISHED, mesos.TASK_RUNNING, mesos.TASK_STAGING, mesos.TASK_STARTING:
		log.Printf("status update from agent %q: %v", s.GetAgentID().GetValue(), st)
		if st != mesos.TASK_FINISHED {
			return nil
		}
	case mesos.TASK_LOST, mesos.TASK_KILLED, mesos.TASK_FAILED, mesos.TASK_ERROR:
		log.Println("Exiting because task " + s.GetTaskID().Value +
			" is in an unexpected state " + st.String() +
			" with reason " + s.GetReason().String() +
			" from source " + s.GetSource().String() +
			" with message '" + s.GetMessage() + "'")
		return ExitError(3)
	default:
		log.Println("unexpected task state, aborting", st)
		return ExitError(4)
	}
	return ExitError(0) // kind of ugly, but better than os.Exit(0)
}

type ExitError int

func (e ExitError) Error() string { return fmt.Sprintf("exit code %d", int(e)) }
