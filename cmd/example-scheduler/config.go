package main

import (
	"flag"
	"time"
)

type config struct {
	id          string
	user        string
	name        string
	role        string
	url         string
	codec       codec
	timeout     time.Duration
	checkpoint  bool
	principal   string
	hostname    string
	labels      Labels
	server      server
	executor    string
	tasks       int
	verbose     bool
	taskCPU     float64
	taskMemory  float64
	execCPU     float64
	execMemory  float64
	reviveBurst int
	reviveWait  time.Duration
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
	fs.Float64Var(&cfg.taskCPU, "cpu", cfg.taskCPU, "CPU resources to consume per-task")
	fs.Float64Var(&cfg.taskMemory, "memory", cfg.taskMemory, "Memory resources (MB) to consume per-task")
	fs.Float64Var(&cfg.execCPU, "exec.cpu", cfg.execCPU, "CPU resources to consume per-executor")
	fs.Float64Var(&cfg.execMemory, "exec.memory", cfg.execMemory, "Memory resources (MB) to consume per-executor")
	fs.IntVar(&cfg.reviveBurst, "revive.burst", cfg.reviveBurst, "Number of revive messages that may be sent in a burst within revive-wait period")
	fs.DurationVar(&cfg.reviveWait, "revive.wait", cfg.reviveWait, "Wait this long to fully recharge revive-burst quota")
}

type server struct {
	address string
	port    int
}
