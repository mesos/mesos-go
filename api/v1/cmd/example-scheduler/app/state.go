package app

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	proto "github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpsched"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
)

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
	} else if execBinary != "" {
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
	return nil, errors.New("must specify an executor binary or image")
}

func buildWantsTaskResources(config Config) (r mesos.Resources) {
	r.Add(
		*mesos.BuildResource().Name("cpus").Scalar(config.taskCPU).Resource,
		*mesos.BuildResource().Name("mem").Scalar(config.taskMemory).Resource,
	)
	log.Println("wants-task-resources = " + r.String())
	return
}

func buildWantsExecutorResources(config Config) (r mesos.Resources) {
	r.Add(
		*mesos.BuildResource().Name("cpus").Scalar(config.execCPU).Resource,
		*mesos.BuildResource().Name("mem").Scalar(config.execMemory).Resource,
	)
	log.Println("wants-executor-resources = " + r.String())
	return
}

func buildHTTPSched(cfg Config, creds credentials) calls.Caller {
	var authConfigOpt httpcli.ConfigOpt
	// TODO(jdef) make this auth-mode configuration more pluggable
	if cfg.authMode == AuthModeBasic {
		log.Println("configuring HTTP Basic authentication")
		// TODO(jdef) this needs testing once mesos 0.29 is available
		authConfigOpt = httpcli.BasicAuth(creds.username, creds.password)
	}
	cli := httpcli.New(
		httpcli.Endpoint(cfg.url),
		httpcli.Codec(cfg.codec.Codec),
		httpcli.Do(httpcli.With(
			authConfigOpt,
			httpcli.Timeout(cfg.timeout),
			httpcli.Transport(func(t *http.Transport) {
				// all calls should be ack'd by the server within this interval.
				// TODO(jdef) it probably doesn't make sense if this value is larger
				// than cfg.timeout.
				t.ResponseHeaderTimeout = 15 * time.Second
				t.MaxIdleConnsPerHost = 2 // don't depend on go's default
			}),
		)),
	)
	if cfg.compression {
		// TODO(jdef) experimental; currently released versions of Mesos will accept this
		// header but will not send back compressed data due to flushing issues.
		log.Println("compression enabled")
		cli.With(httpcli.RequestOptions(httpcli.Header("Accept-Encoding", "gzip")))
	}
	return httpsched.NewCaller(cli)
}

func buildFrameworkInfo(cfg Config) *mesos.FrameworkInfo {
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
	return frameworkInfo
}

func loadCredentials(userConfig credentials) (result credentials, err error) {
	result = userConfig
	if result.password != "" {
		// this is the path to a file containing the password
		_, err = os.Stat(result.password)
		if err != nil {
			return
		}
		var f *os.File
		f, err = os.Open(result.password)
		if err != nil {
			return
		}
		defer f.Close()
		var bytes []byte
		bytes, err = ioutil.ReadAll(f)
		if err != nil {
			return
		}
		result.password = string(bytes)
	}
	return
}

func newInternalState(cfg Config) (*internalState, error) {
	metricsAPI := initMetrics(cfg)
	executorInfo, err := prepareExecutorInfo(
		cfg.executor,
		cfg.execImage,
		cfg.server,
		buildWantsExecutorResources(cfg),
		cfg.jobRestartDelay,
		metricsAPI,
	)
	if err != nil {
		return nil, err
	}
	creds, err := loadCredentials(cfg.credentials)
	if err != nil {
		return nil, err
	}
	state := &internalState{
		config:             cfg,
		totalTasks:         cfg.tasks,
		reviveTokens:       backoff.BurstNotifier(cfg.reviveBurst, cfg.reviveWait, cfg.reviveWait, nil),
		wantsTaskResources: buildWantsTaskResources(cfg),
		executor:           executorInfo,
		metricsAPI:         metricsAPI,
		cli:                buildHTTPSched(cfg, creds),
		random:             rand.New(rand.NewSource(time.Now().Unix())),
	}
	return state, nil
}

type internalState struct {
	tasksLaunched      int
	tasksFinished      int
	totalTasks         int
	frameworkID        string
	role               string
	executor           *mesos.ExecutorInfo
	cli                calls.Caller
	config             Config
	wantsTaskResources mesos.Resources
	reviveTokens       <-chan struct{}
	metricsAPI         *metricsAPI
	err                error
	done               bool
	random             *rand.Rand
}
