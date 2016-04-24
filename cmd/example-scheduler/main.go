package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	proto "github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/backoff"
	"github.com/mesos/mesos-go/cmd"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli"
	"github.com/mesos/mesos-go/httpcli/stream"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/scheduler/calls"
)

func main() {
	cfg := config{
		user:             env("FRAMEWORK_USER", "root"),
		name:             env("FRAMEWORK_NAME", "example"),
		url:              env("MESOS_MASTER_HTTP", "http://:5050/api/v1/scheduler"),
		codec:            codec{Codec: &encoding.ProtobufCodec},
		timeout:          envDuration("MESOS_CONNECT_TIMEOUT", "1s"),
		checkpoint:       true,
		server:           server{address: env("LIBPROCESS_IP", "127.0.0.1")},
		tasks:            envInt("NUM_TASKS", "5"),
		taskCPU:          envFloat("TASK_CPU", "1"),
		taskMemory:       envFloat("TASK_MEMORY", "64"),
		execCPU:          envFloat("EXEC_CPU", "0.01"),
		execMemory:       envFloat("EXEC_MEMORY", "64"),
		reviveBurst:      envInt("REVIVE_BURST", "3"),
		reviveWait:       envDuration("REVIVE_WAIT", "1s"),
		maxRefuseSeconds: envDuration("MAX_REFUSE_SECONDS", "5s"),
		jobRestartDelay:  envDuration("JOB_RESTART_DELAY", "5s"),
		execImage:        env("EXEC_IMAGE", cmd.DockerImageTag),
		executor:         env("EXEC_BINARY", "/opt/example-executor"),
		metrics: metrics{
			port: envInt("PORT0", "64009"),
			path: env("METRICS_API_PATH", "/metrics"),
		},
	}

	fs := flag.NewFlagSet("config", flag.ExitOnError)
	cfg.addFlags(fs)
	fs.Parse(os.Args[1:])

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	if cfg.executor == "" && cfg.execImage == "" {
		return errors.New("must specify an executor binary or image")
	}

	log.Printf("scheduler running with configuration: %+v", cfg)

	metricsAPI := initMetrics(cfg)
	executorInfo, err := prepareExecutorInfo(
		cfg.executor, cfg.execImage, cfg.server, buildWantsExecutorResources(cfg), cfg.jobRestartDelay, metricsAPI)
	if err != nil {
		return err
	}
	state := internalState{
		config:             cfg,
		totalTasks:         cfg.tasks,
		reviveTokens:       backoff.BurstNotifier(cfg.reviveBurst, cfg.reviveWait, cfg.reviveWait, nil),
		wantsTaskResources: buildWantsTaskResources(cfg),
		executor:           executorInfo,
		metricsAPI:         metricsAPI,
		cli: httpcli.New(
			httpcli.URL(cfg.url),
			httpcli.Codec(cfg.codec.Codec),
			httpcli.Do(httpcli.With(httpcli.Timeout(cfg.timeout))),
		),
	}
	if cfg.compression {
		// TODO(jdef) experimental; currently released versions of Mesos will accept this
		// header but will not send back compressed data due to flushing issues.
		log.Println("compression enabled")
		state.cli.With(httpcli.RequestOptions(httpcli.Header("Accept-Encoding", "gzip")))
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
	registrationTokens := backoff.Notifier(1*time.Second, 15*time.Second, nil)
	for {
		state.metricsAPI.subscriptionAttempts()
		resp, opt, err := stream.Subscribe(state.cli, subscribe)
		func() {
			if resp != nil {
				defer resp.Close()
			}
			if err == nil {
				func() {
					undo := state.cli.With(opt)
					defer state.cli.With(undo) // strip the stream options
					err = eventLoop(&state, resp.Decoder())
				}()
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

// returns the framework ID received by mesos (if any); callers should check for a
// framework ID regardless of whether error != nil.
func eventLoop(state *internalState, eventDecoder encoding.Decoder) (err error) {
	state.frameworkID = ""
	callOptions := scheduler.CallOptions{} // should be applied to every outgoing call

	for err == nil && !state.done {
		var e scheduler.Event
		if err = eventDecoder.Invoke(&e); err != nil {
			continue
		}

		if state.config.verbose {
			log.Printf("%+v\n", e)
		}

		switch e.GetType() {
		case scheduler.Event_FAILURE:
			state.metricsAPI.failuresReceived()
			log.Println("received a FAILURE event")
			f := e.GetFailure()
			failure(f.ExecutorID, f.AgentID, f.Status)

		case scheduler.Event_OFFERS:
			if state.config.verbose {
				log.Println("received an OFFERS event")
			}
			offers := e.GetOffers().GetOffers()
			state.metricsAPI.offersReceived.Int(len(offers))
			t := time.Now()
			resourceOffers(state, callOptions.Copy(), offers)
			if state.config.summaryMetrics {
				state.metricsAPI.processOffersLatency.Since(t)
			}

		case scheduler.Event_UPDATE:
			state.metricsAPI.updatesReceived()
			statusUpdate(state, callOptions.Copy(), e.GetUpdate().GetStatus())

		case scheduler.Event_ERROR:
			state.metricsAPI.errorsReceived()
			// it's recommended that we abort and re-try subscribing; setting
			// err here will cause the event loop to terminate and the connection
			// will be reset.
			err = fmt.Errorf("ERROR: " + e.GetError().GetMessage())

		case scheduler.Event_SUBSCRIBED:
			state.metricsAPI.subscribedReceived()
			log.Println("received a SUBSCRIBED event")
			if state.frameworkID == "" {
				state.frameworkID = e.GetSubscribed().GetFrameworkID().GetValue()
				if state.frameworkID == "" {
					// sanity check
					err = errors.New("mesos gave us an empty frameworkID")
					break
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
		resp, err := state.cli.Do(accept)
		if resp != nil {
			resp.Close()
		}
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
		resp, err := state.cli.Do(ack)
		if resp != nil {
			resp.Close()
		}
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
		resp, err := state.cli.Do(calls.Revive().With(callOptions...))
		if resp != nil {
			resp.Close()
		}
		if err != nil {
			state.metricsAPI.apiErrorCount("revive")
			log.Printf("failed to revive offers: %+v", err)
			return
		}
	default:
		// noop
	}
}

func prepareExecutorInfo(
	execBinary, execImage string,
	server server,
	wantsResources mesos.Resources,
	jobRestartDelay time.Duration,
	metricsAPI *metricsAPI,
) (*mesos.ExecutorInfo, error) {
	if execImage != "" {
		// Create mesos custom executor
		return &mesos.ExecutorInfo{
			ExecutorID: mesos.ExecutorID{Value: "default"},
			Name:       proto.String("Test Executor"),
			Command: mesos.CommandInfo{
				Shell: func() *bool { x := false; return &x }(),
			},
			Container: &mesos.ContainerInfo{
				Type: mesos.ContainerInfo_DOCKER.Enum(),
				Docker: &mesos.ContainerInfo_DockerInfo{
					Image:          execImage,
					ForcePullImage: func() *bool { x := true; return &x }(),
					Parameters: []mesos.Parameter{
						{
							Key:   "entrypoint",
							Value: execBinary,
						}}}},
			Resources: wantsResources,
		}, nil
	} else {
		log.Println("No executor image specified, will serve executor binary from built-in HTTP server")

		listener, iport, err := newListener(server)
		if err != nil {
			return nil, err
		}
		server.port = iport // we're just working with a copy of server, so this is OK
		var (
			mux                    = http.NewServeMux()
			executorUris           = []mesos.CommandInfo_URI{}
			uri, executorCmd, err2 = serveExecutorArtifact(server, execBinary, mux)
			executorCommand        = fmt.Sprintf("./%s", executorCmd)
		)
		if err2 != nil {
			return nil, err2
		}
		wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			metricsAPI.artifactDownloads()
			mux.ServeHTTP(w, r)
		})
		executorUris = append(executorUris, mesos.CommandInfo_URI{Value: uri, Executable: proto.Bool(true)})

		go forever("artifact-server", jobRestartDelay, metricsAPI.jobStartCount, func() error { return http.Serve(listener, wrapper) })
		log.Println("Serving executor artifacts...")

		// Create mesos custom executor
		return &mesos.ExecutorInfo{
			ExecutorID: mesos.ExecutorID{Value: "default"},
			Name:       proto.String("Test Executor"),
			Command: mesos.CommandInfo{
				Value: proto.String(executorCommand),
				URIs:  executorUris,
			},
			Resources: wantsResources,
		}, nil
	}
}

func buildWantsTaskResources(config config) (r mesos.Resources) {
	r.Add(
		*mesos.BuildResource().Name("cpus").Scalar(config.taskCPU).Resource,
		*mesos.BuildResource().Name("mem").Scalar(config.taskMemory).Resource,
	)
	log.Println("wants-task-resources = " + r.String())
	return
}

func buildWantsExecutorResources(config config) (r mesos.Resources) {
	r.Add(
		*mesos.BuildResource().Name("cpus").Scalar(config.execCPU).Resource,
		*mesos.BuildResource().Name("mem").Scalar(config.execMemory).Resource,
	)
	log.Println("wants-executor-resources = " + r.String())
	return
}

type internalState struct {
	tasksLaunched      int
	tasksFinished      int
	totalTasks         int
	frameworkID        string
	role               string
	executor           *mesos.ExecutorInfo
	cli                *httpcli.Client
	config             config
	wantsTaskResources mesos.Resources
	reviveTokens       <-chan struct{}
	metricsAPI         *metricsAPI
	err                error
	done               bool
}
