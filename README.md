Go bindings for Apache Mesos
========

Very early version of a pure Go language bindings for Apache Mesos. As with other pure implmentation, mesos-go uses the HTTP wire protocol to communicate directly with  a running Mesos master and its slave instances. One of the objectives of this project is to provide an idiomatic Go API that makes it super easy to create Mesos frameworks using Go. 

[![GoDoc] (https://godoc.org/github.com/mesos/mesos-go?status.png)](https://godoc.org/github.com/mesos/mesos-go)

## Current Status
This is a very early version of the project.  Howerver, here is a list of things that works so far:

- The SchedulerDriver API implemented
- The ExecutorDriver API implemented
- Stable API (based on the core Mesos code)
- Plenty of unit and integrative of tests
- Modular design for easy readability/extensibility
- Example programs on how to use the API

##### Work in Progress...
- Leading master detection
- Authentication/Authorization via SASL

## Pre-Requisites
- Go 1.3 or higher
- A standard and working Go workspace setup
- Install Protocol Buffer tools 2.5 or higher locally - See http://code.google.com/p/protobuf/
- Apache Mesos 0.19 or newer
- GNU Make
- [godep](https://github.com/tools/godep)

## Build Instructions
The following instructions is to build the code from `github`.The project uses the `GoDep` for dependency management.
```
$ cd <go-workspace>/src/
$ mkdir -p github.com/mesos
$ cd github.com/mesos
$ git clone https://github.com/mesos/mesos-go.git
$ cd mesos-go
$ go get github.com/tools/godep
$ godep restore
$ go build ./...
```
The previous will build the code base.  

### Building the Examples
Use the following steps to build the example scheduler and executor:
```
$ cd <go-workspace>/src/github.com/mesos/mesos-go
$ cd examples

# build test-framework
$ go build -tags=test-sched -o test-framework test_framework.go

# build test-executor
$ go build -tags=test-exec -o test-executor test_framework.go
```
## Running the Example
### Start Mesos
You will need a running Mesos master and slaves to run the examples.   For instance, start a local Mesos: 
```
$ <mesos-build-install>/bin/mesos-local --ip=127.0.0.1 --port=5050
```
See http://mesos.apache.org/gettingstarted/ for getting started with Apache Mesos.

### Running the Go Scheduler/Executor Examples
```
$ cd <go-workspace>/src/github.com/mesos/mesos-go
$ cd examples
$ ./test-framework --master=127.0.0.1:5050 --executor="<go-workspace>/src/github.com/mesos/mesos-go/examples/test-executor" --logtostderr=true
```
Note: you must provide the fully-qualified path to the `test-executor` binary.  If all goes well, you should see output about task completion.  You can also point your browser to the Mesos GUI http://127.0.0.1:5050/ to validate the framework activities.

### Running the Go Scheduler with Other Executors
You can also use the Go `test-framework` with executors written in other languages such as  `Python` or `Java`  for further validation (note: to use these executors requires a build of the mesos source code with `make check`):
```
$ ./test-framework --master=127.0.0.1:5050 --executor="<mesos-build>/src/examples/python/test-executor" --logtostderr=true
```
Similarly for the Java version:
```
$ ./test-framework --master=127.0.0.1:5050 --executor="<mesos-build>/src/examples/java/test-executor" --logtostderr=true
```
