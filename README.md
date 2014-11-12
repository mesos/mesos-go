Go bindings for Apache Mesos
========

Very early version of a pure Go language bindings for Apache Mesos. As with other pure implmentation, mesos-go uses the HTTP wire protocol to communicate directly with  a running Mesos master and its slave instances. One of the objectives of this project is to provide an idiomatic Go API that makes it super easy to create Mesos frameworks using Go. 

[![GoDoc] (https://godoc.org/github.com/mesos/mesos-go?status.png)](https://godoc.org/github.com/mesos/mesos-go)

## Current Status
This is a very early version of the project.  Howerver, here is a list of things that works so far
- The SchedulerDriver API implemented
- The ExecutorDriver API implemented
- Tons of tests
- Examples of how to use the API

##### Work in Progress...
- APIs are still taking shape, but getting stable
- No Master detection via Zookeeper
- No Auth via SASL
- Visit issues list for detail

## Pre-Requisites
- Go 1.3 or higher
- A standard and working Go workspace setup
- Install Protocol Buffer tools 2.5 or higher locally - See http://code.google.com/p/protobuf/
- Apache Mesos 0.19 or newer


## Build Instructions
The following instructions is to build the code from a github pull.  The code resides in branch labeled `pure` and uses the `GoDep` project for dependency management.
```
git clone https://github.com/mesos/mesos-go.git -b pure
$ go get github.com/tools/godep
$ cd mesos-go
$ godep restore
$ go build ./...
```
## Running Example
The codebase comes with an example of a scheduler and executor found in the `examples` directory.
The examples need a running mesos master and slaves to work properly.   For instance, start local-mesos 
```
$ <mesos-install>/bin/mesos-local --ip=127.0.0.1 --port=5050
```
### Running the Scheduler
* Change directory to `examples`
* Build the scheduler binary `$ go build -o sample_framework test_framework.go`
* Build the executor binary `$ go build -o sample_exec test_executor.go`
* Ensure your mesos instance is running, then launch scheduler
```
$ ./sample_framework --master=127.0.0.1:5050 --executor="<fully-qualified-path-to>/sample_exec" --logtostderr=true -v=99
```
Note that you must provide the fully-qualified path to the executor binary.