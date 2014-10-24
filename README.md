Go bindings for Apache Mesos
========

Very early version of a pure Go language bindings for Apache Mesos. As with other pure implmentation, mesos-go uses the HTTP wire protocol to communicate directly with  a running Mesos master and its slave instances. One of the objectives of this project is to provide an idiomatic Go API that makes it super easy to create Mesos frameworks using Go. 

[![GoDoc] (https://godoc.org/github.com/mesos/mesos-go?status.png)](https://godoc.org/github.com/mesos/mesos-go)

## Current Status
This is a very early version of the project.  Howerver, here is a list of things that works so far
- The SchedulerDriver API implemented
- The ExecutorDriver API implemented
- Logging using Google Log
- Idiomatic Go

##### Work in Progress
- APIs are unstable and still taking shape
- Does not support zookeeper yet (no master detection yet)
- Authentication (Soon)
- SASL (not yet)
- Hardening of code with more tests
- See more in issues list ...


## Pre-Requisites
- Go 1.3 or higher
- A standard Go workspace setup
- Install Protocol Buffer tools 2.5 or higher locally - See http://code.google.com/p/protobuf/
- Apache Mesos 0.19 or newer


## Build Instructions
* Pull `pure` branch: `git clone https://github.com/mesos/mesos-go.git -b pure`
* The project uses GoDep. See https://github.com/tools/godep 
  - Install godep command `$ go get github.com/tools/godep`
  - From `mesos-go` project root, restore dependendencies `$ godep restore`
  - This will download and install all go dependencies for the project.
* Build all components of project `$ go build ./...`

## Running Example
The codebase comes with an example of a scheduler and executor found in the `examples` directory.
The examples need a running mesos master and slaves running.   For instance, start local-mesos `$ <mesoe-install>/bin/mesos-local --ip=127.0.0.1 --port=5050`

### Running the Scheduler
* Change directory to examples/sched
* Build the scheduler if not already built with `go build .`
* Ensure your mesos instance is running with a known master address. 
* Next, start the scheduler `$./sched --ip=127.0.0.1 --port=5050 --logtostderr=true`
* When running, you should see the scheduler launching tasks

## Running the Executor
For now, the sample executor relies on ENV vars for configuration.  This may change in future version.  You will need to set the followings:
* `export MESOS_SLAVE_PID="slave(1)@127.0.0.1:5050"` - or get PID from mesos-local start log.
* `export MESOS_SLAVE_ID="test-slave-001"`
* `export MESOS_FRAMEWORK_ID="mesos-framework-1"`
* `export MESOS_EXECUTOR_ID="mesos-executor-1"`
* When running you wil see the executor accepting handling launch tasks.