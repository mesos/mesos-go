// msh is a minimal mesos v1 scheduler; it executes a shell command on a mesos agent.
package app

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/agent"
	agentcalls "github.com/mesos/mesos-go/api/v1/lib/agent/calls"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/callrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/controller"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/eventrules"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/offers"
	"github.com/mesos/mesos-go/api/v1/lib/extras/store"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpagent"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/resources"
	"github.com/mesos/mesos-go/api/v1/lib/roles"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
)

const (
	RFC3339a = "20060102T150405Z0700"
)

type Config struct {
	FrameworkName string
	TaskName      string
	MesosMaster   string // MesosMaster is formatted as host:port
	User          string
	Role          string
	CPUs          float64
	Memory        float64
	TTY           bool
	Pod           bool
	Interactive   bool
	Command       []string // Command must not be empty.
	Log           func(string, ...interface{})
	Silent        bool
}

func DefaultConfig() Config {
	return Config{
		FrameworkName: "msh",
		TaskName:      "msh",
		MesosMaster:   "127.0.0.1:5050",
		User:          "root",
		Role:          "*",
		CPUs:          float64(0.010),
		Memory:        float64(64),
	}
}

func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.FrameworkName, "framework_name", c.FrameworkName, "Name of the framework")
	fs.StringVar(&c.TaskName, "task_name", c.TaskName, "Name of the msh task")
	fs.StringVar(&c.MesosMaster, "master", c.MesosMaster, "IP:port of the mesos master")
	fs.StringVar(&c.User, "user", c.User, "OS user that owns the launched task")
	fs.Float64Var(&c.CPUs, "cpus", c.CPUs, "CPU resources to allocate for the remote command")
	fs.Float64Var(&c.Memory, "memory", c.Memory, "Memory resources to allocate for the remote command")
	fs.BoolVar(&c.TTY, "tty", c.TTY, "Route all container stdio, stdout, stderr communication through a TTY device")
	fs.BoolVar(&c.Pod, "pod", c.Pod, "Launch the remote command in a mesos task-group")
	fs.BoolVar(&c.Interactive, "interactive", c.Interactive, "Attach to the task's stdin, stdout, and stderr")
	fs.BoolVar(&c.Silent, "silent", c.Silent, "Log nothing to stdout/stderr")
}

var (
	refuseSeconds = calls.RefuseSeconds(5 * time.Second)
)

type App struct {
	Config
	fidStore               store.Singleton
	declineAndSuppress     bool
	wantsResources         mesos.Resources
	taskPrototype          mesos.TaskInfo
	executorPrototype      mesos.ExecutorInfo
	wantsExecutorResources mesos.Resources
	agentDirectory         map[mesos.AgentID]string
	uponExit               *cleanups
}

func New(c Config) *App {
	if c.Log == nil {
		if c.Silent {
			// swallow all log output
			c.Log = func(string, ...interface{}) {}
		} else {
			c.Log = log.Printf
		}
	}
	app := &App{
		Config: c,
		wantsExecutorResources: mesos.Resources{
			resources.NewCPUs(0.01).Resource,
			resources.NewMemory(32).Resource,
			resources.NewDisk(5).Resource,
		},
		agentDirectory: make(map[mesos.AgentID]string),
		uponExit:       new(cleanups),
		fidStore: store.DecorateSingleton(
			store.NewInMemorySingleton(),
			store.DoSet().AndThen(func(_ store.Setter, v string, _ error) error {
				c.Log("FrameworkID %q", v)
				return nil
			})),
		wantsResources: mesos.Resources{
			resources.NewCPUs(c.CPUs).Resource,
			resources.NewMemory(c.Memory).Resource,
		},
		taskPrototype: mesos.TaskInfo{
			Name: c.TaskName,
			Command: &mesos.CommandInfo{
				Value:     proto.String(c.Command[0]),
				Shell:     proto.Bool(false),
				Arguments: c.Command,
			},
		},
	}
	if c.Interactive {
		app.taskPrototype.Container = &mesos.ContainerInfo{
			Type:    mesos.ContainerInfo_MESOS.Enum(),
			TTYInfo: &mesos.TTYInfo{},
		}
	}
	if term := os.Getenv("TERM"); term != "" && c.TTY {
		app.taskPrototype.Command.Environment = &mesos.Environment{
			Variables: []mesos.Environment_Variable{
				mesos.Environment_Variable{Name: "TERM", Value: &term},
			},
		}
	}
	return app
}

func (app *App) Run(ctx context.Context) error {
	defer app.uponExit.unwind()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	caller := callrules.WithFrameworkID(store.GetIgnoreErrors(app.fidStore)).Caller(app.buildClient())

	return controller.Run(
		ctx,
		&mesos.FrameworkInfo{User: app.User, Name: app.FrameworkName, Role: proto.String(app.Role)},
		caller,
		controller.WithContextPerSubscription(true),
		controller.WithEventHandler(app.buildEventHandler(caller)),
		controller.WithFrameworkID(store.GetIgnoreErrors(app.fidStore)),
		controller.WithSubscriptionTerminated(func(err error) {
			cancel()
			if err == io.EOF {
				app.Log("disconnected")
			}
		}),
	)
}

func (app *App) buildClient() calls.Caller {
	return httpsched.NewCaller(httpcli.New(
		httpcli.Endpoint(fmt.Sprintf("http://%s/api/v1/scheduler", app.MesosMaster)),
	))
}

func (app *App) buildEventHandler(caller calls.Caller) events.Handler {
	logger := controller.LogEvents(func(e *scheduler.Event) {
		app.Log("event %v", e)
	})
	return controller.LiftErrors().Handle(events.Handlers{
		scheduler.Event_SUBSCRIBED: eventrules.Rules{
			logger,
			controller.TrackSubscription(app.fidStore, 0),
			app.updateExecutor,
		},

		scheduler.Event_OFFERS: eventrules.Rules{
			app.trackAgents,
			app.maybeDeclineOffers(caller),
			eventrules.DropOnError(),
			eventrules.Handle(app.resourceOffers(caller)),
		},

		scheduler.Event_UPDATE: controller.AckStatusUpdates(caller).AndThen().HandleF(app.statusUpdate),
	}.Otherwise(logger.HandleEvent))
}

func (app *App) updateExecutor(ctx context.Context, e *scheduler.Event, err error, chain eventrules.Chain) (context.Context, *scheduler.Event, error) {
	if err != nil {
		return chain(ctx, e, err)
	}
	if e.GetType() != scheduler.Event_SUBSCRIBED {
		return chain(ctx, e, err)
	}
	if app.Pod {
		app.executorPrototype = mesos.ExecutorInfo{
			Type:        mesos.ExecutorInfo_DEFAULT,
			FrameworkID: e.GetSubscribed().FrameworkID,
		}
	}
	return chain(ctx, e, err)
}

func (app *App) trackAgents(ctx context.Context, e *scheduler.Event, err error, chain eventrules.Chain) (context.Context, *scheduler.Event, error) {
	if err != nil {
		return chain(ctx, e, err)
	}
	if e.GetType() != scheduler.Event_OFFERS {
		return chain(ctx, e, err)
	}
	off := e.GetOffers().GetOffers()
	for i := range off {
		// TODO(jdef) eventually implement an algorithm to purge agents that are gone
		app.agentDirectory[off[i].GetAgentID()] = off[i].GetHostname()
	}
	return chain(ctx, e, err)
}

func (app *App) maybeDeclineOffers(caller calls.Caller) eventrules.Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, chain eventrules.Chain) (context.Context, *scheduler.Event, error) {
		if err != nil {
			return chain(ctx, e, err)
		}
		if e.GetType() != scheduler.Event_OFFERS || !app.declineAndSuppress {
			return chain(ctx, e, err)
		}
		off := offers.Slice(e.GetOffers().GetOffers())
		err = calls.CallNoData(ctx, caller, calls.Decline(off.IDs()...).With(refuseSeconds))
		if err == nil {
			// we shouldn't have received offers, maybe the prior suppress call failed?
			err = calls.CallNoData(ctx, caller, calls.Suppress())
		}
		return ctx, e, err // drop
	}
}

func (app *App) resourceOffers(caller calls.Caller) events.HandlerFunc {
	return func(ctx context.Context, e *scheduler.Event) (err error) {
		var (
			off            = e.GetOffers().GetOffers()
			index          = offers.NewIndex(off, nil)
			matchResources = func() mesos.Resources {
				if app.Pod {
					return app.wantsResources.Plus(app.wantsExecutorResources...)
				} else {
					return app.wantsResources
				}
			}()
			match = index.Find(offers.ContainsResources(matchResources))
		)
		if match != nil {
			ts := time.Now().Format(RFC3339a)
			task := app.taskPrototype
			task.TaskID = mesos.TaskID{Value: ts}
			task.AgentID = match.AgentID
			task.Resources = resources.Find(
				resources.Flatten(app.wantsResources, roles.Role(app.Role).Assign()),
				match.Resources...,
			)

			if app.Pod {
				executor := app.executorPrototype
				executor.ExecutorID = mesos.ExecutorID{Value: "msh_" + ts}
				executor.Resources = resources.Find(
					resources.Flatten(app.wantsExecutorResources, roles.Role(app.Role).Assign()),
					match.Resources...,
				)
				err = calls.CallNoData(ctx, caller, calls.Accept(
					calls.OfferOperations{calls.OpLaunchGroup(executor, task)}.WithOffers(match.ID),
				))
			} else {
				err = calls.CallNoData(ctx, caller, calls.Accept(
					calls.OfferOperations{calls.OpLaunch(task)}.WithOffers(match.ID),
				))
			}
			if err != nil {
				return
			}

			app.declineAndSuppress = true
		} else {
			app.Log("rejected insufficient offers")
		}
		// decline all but the possible match
		delete(index, match.GetID())
		err = calls.CallNoData(ctx, caller, calls.Decline(index.IDs()...).With(refuseSeconds))
		if err != nil {
			return
		}
		if app.declineAndSuppress {
			err = calls.CallNoData(ctx, caller, calls.Suppress())
		}
		return
	}
}

func (app *App) statusUpdate(ctx context.Context, e *scheduler.Event) error {
	s := e.GetUpdate().GetStatus()
	switch st := s.GetState(); st {
	case mesos.TASK_FINISHED, mesos.TASK_RUNNING, mesos.TASK_STAGING, mesos.TASK_STARTING:
		app.Log("status update from agent %q: %v", s.GetAgentID().GetValue(), st)
		if st == mesos.TASK_RUNNING && app.Interactive && s.AgentID != nil {
			cid := s.GetContainerStatus().GetContainerID()
			if cid != nil {
				app.Log("attaching for interactive session to agent %q container %q", s.AgentID.Value, cid.Value)
				return app.tryInteractive(ctx, app.agentDirectory[*s.AgentID], *cid)
			}
		}
		if st != mesos.TASK_FINISHED {
			return nil
		}
	case mesos.TASK_LOST, mesos.TASK_KILLED, mesos.TASK_FAILED, mesos.TASK_ERROR:
		app.Log("Exiting because task " + s.GetTaskID().Value +
			" is in an unexpected state " + st.String() +
			" with reason " + s.GetReason().String() +
			" from source " + s.GetSource().String() +
			" with message '" + s.GetMessage() + "'")
		return ExitError(3)
	default:
		app.Log("unexpected task state, aborting %v", st)
		return ExitError(4)
	}
	return ExitError(0) // kind of ugly, but better than os.Exit(0)
}

type ExitError int

func (e ExitError) Error() string { return fmt.Sprintf("exit code %d", int(e)) }

func (app *App) tryInteractive(ctx context.Context, agentHost string, cid mesos.ContainerID) (err error) {
	// TODO(jdef) only re-attach if we're disconnected (guard against redundant TASK_RUNNING)
	ctx, cancel := context.WithCancel(ctx)
	var winCh <-chan mesos.TTYInfo_WindowSize
	if app.TTY {
		ttyd, err := initTTY(ttyLogger(app.Log))
		if err != nil {
			cancel() // stop go-vet from complaining
			return err
		}

		app.uponExit.push(ttyd.Close) // fail-safe

		go func() {
			<-ctx.Done()
			//println("closing ttyd via ctx.Done")
			ttyd.Close()
		}()

		winCh = ttyd.winch
	}

	var (
		cli = httpagent.NewSender(
			httpcli.New(
				httpcli.Endpoint(fmt.Sprintf("http://%s/api/v1", net.JoinHostPort(agentHost, "5051"))),
			).Send,
		)
		aciCh = make(chan *agent.Call, 1) // must be buffered to avoid blocking below
	)
	aciCh <- agentcalls.AttachContainerInput(cid) // very first input message MUST be this
	go func() {
		defer cancel()
		acif := agentcalls.FromChan(aciCh)

		// blocking call, hence the goroutine; Send only returns when the input stream is severed
		err2 := agentcalls.SendNoData(ctx, cli, acif)
		if err2 != nil && err2 != io.EOF {
			app.Log("attached input stream error %v", err2)
		}
	}()

	// attach to container stdout, stderr; Send returns immediately with a Response from which output
	// may be decoded.
	output, err := cli.Send(ctx, agentcalls.NonStreaming(agentcalls.AttachContainerOutput(cid)))
	if err != nil {
		app.Log("attach output stream error: %v", err)
		if output != nil {
			output.Close()
		}
		cancel()
		return
	}

	go func() {
		defer cancel()
		attachContainerOutput(output, os.Stdout, os.Stderr)
	}()

	go attachContainerInput(ctx, os.Stdin, winCh, aciCh)

	return nil
}

func attachContainerInput(ctx context.Context, stdin io.Reader, winCh <-chan mesos.TTYInfo_WindowSize, aciCh chan<- *agent.Call) {
	defer close(aciCh)

	input := make(chan []byte)
	go func() {
		defer close(input)
		escape := []byte{0x10, 0x11} // CTRL-P, CTRL-Q
		var last byte
		for {
			buf := make([]byte, 512) // not efficient to always do this
			n, err := stdin.Read(buf)
			if n > 0 {
				if (last == escape[0] && buf[0] == escape[1]) || bytes.Index(buf, escape) > -1 {
					//println("escape sequence detected")
					return
				}
				buf = buf[:n]
				last = buf[n-1]
				select {
				case input <- buf:
				case <-ctx.Done():
					return
				}
			}
			// TODO(jdef) check for temporary error?
			if err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		// TODO(jdef) send a heartbeat message every so often
		// attach_container_input process_io heartbeats may act as keepalive's, `interval` field is ignored:
		// https://github.com/apache/mesos/blob/4e200e55d8ed282b892f650983ebdf516680d90d/src/slave/containerizer/mesos/io/switchboard.cpp#L1608
		case data, ok := <-input:
			if !ok {
				return
			}
			c := agentcalls.AttachContainerInputData(data)
			select {
			case aciCh <- c:
			case <-ctx.Done():
				return
			}
		case ws := <-winCh:
			c := agentcalls.AttachContainerInputTTY(&mesos.TTYInfo{WindowSize: &ws})
			select {
			case aciCh <- c:
			case <-ctx.Done():
				return
			}
		}
	}
}

func attachContainerOutput(resp mesos.Response, stdout, stderr io.Writer) error {
	defer resp.Close()
	forward := func(b []byte, out io.Writer) error {
		n, err := out.Write(b)
		if err == nil && len(b) != n {
			err = io.ErrShortWrite
		}
		return err
	}
	for {
		var pio agent.ProcessIO
		err := resp.Decode(&pio)
		if err != nil {
			return err
		}
		switch pio.GetType() {
		case agent.ProcessIO_DATA:
			data := pio.GetData()
			switch data.GetType() {
			case agent.ProcessIO_Data_STDOUT:
				if err := forward(data.GetData(), stdout); err != nil {
					return err
				}
			case agent.ProcessIO_Data_STDERR:
				if err := forward(data.GetData(), stderr); err != nil {
					return err
				}
			default:
				// ignore
			}
		default:
			// ignore
		}
	}
}
