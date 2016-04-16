package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	proto "github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli"
	"github.com/mesos/mesos-go/httpcli/stream"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/scheduler/calls"
)

func env(key, defaultValue string) (value string) {
	if value = os.Getenv(key); value == "" {
		value = defaultValue
	}
	return
}

func envint(key, defaultValue string) int {
	value, err := strconv.Atoi(env(key, defaultValue))
	if err != nil {
		panic(err.Error())
	}
	return value
}

func main() {
	cfg := config{
		user:       env("FRAMEWORK_USER", "root"),
		name:       env("FRAMEWORK_NAME", "example"),
		url:        env("MESOS_MASTER_HTTP", "http://:5050/api/v1/scheduler"),
		codec:      codec{Codec: &encoding.ProtobufCodec},
		timeout:    time.Second,
		checkpoint: true,
		server:     server{address: env("LIBPROCESS_IP", "127.0.0.1")},
		tasks:      envint("NUM_TASKS", "5"),
	}

	fs := flag.NewFlagSet("config", flag.ExitOnError)
	cfg.addFlags(fs)
	fs.Parse(os.Args[1:])

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	if cfg.executor == "" {
		panic("must specify an executor binary")
	}

	state := internalState{
		config: cfg,
		cli: httpcli.New(
			httpcli.URL(cfg.url),
			httpcli.Codec(cfg.codec.Codec),
			httpcli.Do(httpcli.With(httpcli.Timeout(cfg.timeout))),
		),
		executor:   prepareExecutorInfo(cfg.executor, cfg.server),
		totalTasks: cfg.tasks,
	}

	frameworkInfo := &mesos.FrameworkInfo{
		User:       cfg.user,
		Name:       cfg.name,
		Checkpoint: &cfg.checkpoint,
	}
	if cfg.role != "" {
		frameworkInfo.Role = &cfg.role
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
	subscribe := calls.Subscribe(true, frameworkInfo)
	registrationTokens := backoffBucket(1*time.Second, 15*time.Second, nil)
	for {
		resp, opt, err := stream.Subscribe(state.cli, subscribe)
		if err == nil {
			func() {
				undo := state.cli.With(opt)
				defer func() {
					resp.Close()
					state.cli.With(undo) // strip the stream options
				}()
				err = eventLoop(&state, resp.Decoder)
			}()
		}
		if err != nil && err != io.EOF {
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
func eventLoop(state *internalState, eventDecoder encoding.Decoder) (err error) {
	state.frameworkID = ""
	callOptions := scheduler.CallOptions{} // should be applied to every outgoing call

	for err == nil {
		var e scheduler.Event
		if err = eventDecoder.Invoke(&e); err != nil {
			continue
		}

		if state.config.verbose {
			log.Printf("%+v\n", e)
		}

		switch e.GetType() {
		case scheduler.Event_FAILURE:
			log.Println("received a FAILURE event")
			f := e.GetFailure()
			failure(f.ExecutorID, f.AgentID, f.Status)

		case scheduler.Event_OFFERS:
			if state.config.verbose {
				log.Println("received an OFFERS event")
			}
			resourceOffers(state, callOptions.Copy(), e.GetOffers().GetOffers())

		case scheduler.Event_UPDATE:
			statusUpdate(state, callOptions.Copy(), e.GetUpdate().GetStatus())

		case scheduler.Event_ERROR:
			// it's recommended that we abort and re-try subscribing; setting
			// err here will cause the event loop to terminate and the connection
			// will be reset.
			err = fmt.Errorf("ERROR: " + e.GetError().GetMessage())

		case scheduler.Event_SUBSCRIBED:
			log.Println("received a SUBSCRIBED event")
			if state.frameworkID == "" {
				state.frameworkID = e.GetSubscribed().GetFrameworkID().GetValue()
				if state.frameworkID == "" {
					// sanity check
					panic("mesos gave us an empty frameworkID")
				}
				callOptions = append(callOptions, calls.Framework(state.frameworkID))
			}
			// else, ignore subsequently received events like this on the same connection
		default:
			// handle unknown event
		}
	} // for
	return err
} // eventLoop

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

const (
	CPUS_PER_TASK = 1
	MEM_PER_TASK  = 64
)

var wantsTaskResources = func() (r mesos.Resources) {
	r.Add(
		*mesos.BuildResource().Name("cpus").Scalar(CPUS_PER_TASK).Resource,
		*mesos.BuildResource().Name("mem").Scalar(MEM_PER_TASK).Resource,
	)
	log.Println("wants-task-resources = " + r.String())
	return
}()

func resourceOffers(state *internalState, callOptions scheduler.CallOptions, offers []mesos.Offer) {
	for i := range offers {
		var (
			remaining = mesos.Resources(offers[i].Resources)
			tasks     = []mesos.TaskInfo{}
		)

		if state.config.verbose {
			log.Println("received offer id '" + offers[i].ID.Value + "' with resources " + remaining.String())
		}

		wantsResources := wantsTaskResources.Clone()
		if len(offers[i].ExecutorIDs) == 0 {
			wantsResources.Add(state.executor.Resources...)
		}

		for state.tasksLaunched < state.totalTasks && remaining.Flatten().ContainsAll(wantsResources) {
			state.tasksLaunched++
			taskID := state.tasksLaunched

			if state.config.verbose {
				log.Println("launching task " + strconv.Itoa(taskID) + " using offer " + offers[i].ID.Value)
			}

			task := mesos.TaskInfo{TaskID: mesos.TaskID{Value: strconv.Itoa(taskID)}}
			task.Name = "Task " + task.TaskID.Value
			task.AgentID = offers[i].AgentID
			task.Executor = state.executor
			task.Resources = remaining.Find(wantsTaskResources.Flatten(mesos.Role(state.role).Assign()))

			remaining.Subtract(task.Resources...)
			tasks = append(tasks, task)
		}

		// build Accept call to launch all of the tasks we've assembled
		accept := calls.Accept(
			calls.OfferWithOperations(
				offers[i].ID,
				calls.OpLaunch(tasks...),
			),
		).With(callOptions...)

		// send Accept call to mesos
		resp, err := state.cli.Do(accept)
		if err != nil {
			log.Printf("failed to launch tasks: %+v", err)
		} else {
			resp.Close() // no data for these calls
		}
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

		// send Accept call to mesos
		resp, err := state.cli.Do(ack)
		if err != nil {
			log.Printf("failed to ack status update for task: %+v", err)
			return
		} else {
			resp.Close() // no data for these calls
		}
	}
	switch st := s.GetState(); st {
	case mesos.TASK_FINISHED:
		state.tasksFinished++
	case mesos.TASK_LOST, mesos.TASK_KILLED, mesos.TASK_FAILED:
		log.Println("Exiting because task " + s.GetTaskID().Value +
			" is in an unexpected state " + st.String() +
			" with reason " + s.GetReason().String() +
			" from source " + s.GetSource().String() +
			" with message '" + s.GetMessage() + "'")
		os.Exit(1)
	}

	if state.tasksFinished == state.totalTasks {
		log.Println("mission accomplished, terminating")
		os.Exit(0)
	}
	// not done yet, revive offers!
	resp, err := state.cli.Do(calls.Revive().With(callOptions...))
	if err != nil {
		log.Printf("failed to revive offers: %+v", err)
		return
	}
	resp.Close()
}

// returns (downloadURI, basename(path))
func serveExecutorArtifact(server server, path string) (string, string) {
	serveFile := func(pattern string, filename string) {
		http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filename)
		})
	}

	// Create base path (http://foobar:5000/<base>)
	pathSplit := strings.Split(path, "/")
	var base string
	if len(pathSplit) > 0 {
		base = pathSplit[len(pathSplit)-1]
	} else {
		base = path
	}
	serveFile("/"+base, path)

	hostURI := fmt.Sprintf("http://%s:%d/%s", server.address, server.port, base)
	log.Println("Hosting artifact '" + path + "' at '" + hostURI + "'")

	return hostURI, base
}

var wantsExecutorResources = func() (r mesos.Resources) {
	r.Add(
		*mesos.BuildResource().Name("cpus").Scalar(0.01).Resource,
		*mesos.BuildResource().Name("mem").Scalar(64).Resource,
	)
	log.Println("wants-executor-resources = " + r.String())
	return
}()

func prepareListener(server server) (*net.TCPListener, int, error) {
	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(server.address, strconv.Itoa(server.port)))
	if err != nil {
		return nil, 0, err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, 0, err
	}
	bindAddress := listener.Addr().String()
	_, port, err := net.SplitHostPort(bindAddress)
	if err != nil {
		return nil, 0, err
	}
	iport, err := strconv.Atoi(port)
	if err != nil {
		return nil, 0, err
	}
	return listener, iport, nil
}

func prepareExecutorInfo(execBinary string, server server) *mesos.ExecutorInfo {
	listener, iport, err := prepareListener(server)
	if err != nil {
		panic(err.Error()) // TODO(jdef) fixme
	}
	server.port = iport // we're just working with a copy of server, so this is OK
	var (
		executorUris     = []mesos.CommandInfo_URI{}
		uri, executorCmd = serveExecutorArtifact(server, execBinary)
		executorCommand  = fmt.Sprintf("./%s", executorCmd)
	)
	executorUris = append(executorUris, mesos.CommandInfo_URI{Value: uri, Executable: proto.Bool(true)})

	go http.Serve(listener, nil)
	log.Println("Serving executor artifacts...")

	// Create mesos scheduler driver.
	return &mesos.ExecutorInfo{
		ExecutorID: mesos.ExecutorID{Value: "default"},
		Name:       proto.String("Test Executor"),
		Source:     proto.String("foo"),
		Command: mesos.CommandInfo{
			Value: proto.String(executorCommand),
			URIs:  executorUris,
		},
		Resources: wantsExecutorResources,
	}
}

type server struct {
	address string
	port    int
}

type config struct {
	id         string
	user       string
	name       string
	role       string
	url        string
	codec      codec
	timeout    time.Duration
	checkpoint bool
	principal  string
	hostname   string
	labels     Labels
	server     server
	executor   string
	tasks      int
	verbose    bool
}

func (cfg *config) addFlags(fs *flag.FlagSet) {
	fs.StringVar(&cfg.user, "user", cfg.user, "Framework user to register with the Mesos master")
	fs.StringVar(&cfg.name, "name", cfg.name, "Framework name to register with the Mesos master")
	fs.StringVar(&cfg.role, "role", cfg.role, "Framework role to register with the Mesos master")
	fs.Var(&cfg.codec, "codec", "Codec to encode/decode scheduler API communications [protobuf, json]")
	fs.StringVar(&cfg.url, "url", cfg.url, "Mesos scheduler API URL")
	fs.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "Mesos scheduler API connection timeout")
	fs.BoolVar(&cfg.checkpoint, "checkpoint", cfg.checkpoint, "Enable/disable framework checkpointing")
	fs.StringVar(&cfg.principal, "principal", cfg.principal, "Framework principal with which to authenticate")
	fs.StringVar(&cfg.hostname, "hostname", cfg.hostname, "Framework hostname that is advertised to the master")
	fs.Var(&cfg.labels, "label", "Framework label, may be specified multiple times")
	fs.StringVar(&cfg.server.address, "server.address", cfg.server.address, "IP of artifact server")
	fs.IntVar(&cfg.server.port, "server.port", cfg.server.port, "Port of artifact server")
	fs.StringVar(&cfg.executor, "executor", cfg.executor, "Full path to executor binary")
	fs.IntVar(&cfg.tasks, "tasks", cfg.tasks, "Number of tasks to spawn")
	fs.BoolVar(&cfg.verbose, "verbose", cfg.verbose, "Verbose logging")
}

type internalState struct {
	tasksLaunched int
	tasksFinished int
	totalTasks    int
	frameworkID   string
	role          string
	executor      *mesos.ExecutorInfo
	cli           *httpcli.Client
	config        config
}
