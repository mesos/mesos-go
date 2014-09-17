Go bindings for Apache Mesos
========

Early Go language bindings for Apache Mesos.

## Install instructions

ssh into your current leading master node.  
Record the IP of the HDFS name node to publish your built executor.


### Fetch and build the bindings and examples

goprotobuf will be built automatically as a dependency.  
You will also need protobuf >= 2.5, as well as GCC

```bash
go get -v github.com/mesosphere/mesos-go/example_framework/...
```

### Install example executor in HDFS

```bash
hdfs dfs -mkdir /go-tmp
hdfs dfs -put $GOPATH/bin/example_executor /go-tmp
```

### Run example framework:

```bash
$ cd $GOPATH
$ ./bin/example_framework -executor-uri hdfs://<hdfs-name-node>/go-tmp/example_executor
Launching task: 5
Received task status: Go task is running!
Received task status: Go task is done!
Launching task: 4
Received task status: Go task is running!
Received task status: Go task is done!
Launching task: 3
Received task status: Go task is running!
Received task status: Go task is done!
Launching task: 2
Received task status: Go task is running!
Received task status: Go task is done!
Launching task: 1
Received task status: Go task is running!
Received task status: Go task is done!
```

