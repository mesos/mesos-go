Go bindings for Apache Mesos
========

Early Go language bindings for Apache Mesos.

## Install instructions

ssh into your current leading master node.
Record the IP of the HDFS name node to publish your built executor.

### Install build dependencies

On Ubuntu and Debian systems, run:

    $ sudo aptitude install make g++ libprotobuf-dev mercurial

In case your distribution does not include Go 1.1.2+, please fetch a more recent version as certain parts of the protobuf library depends on it:

    $ wget https://go.googlecode.com/files/go1.2.linux-amd64.tar.gz
    $ tar -xvzf go1.2.linux-amd64.tar.gz
    $ echo 'export PATH=~/go/bin:$PATH' >> ~/.bashrc
    $ echo 'export GOROOT=~/go' >> ~/.bashrc
    $ source ~/.bashrc

### Fetch and compile example framework and executor

    $ wget https://github.com/mesosphere/mesos-go/archive/master.zip
    $ unzip master.zip
    $ cd mesos-go-master
    $ export GOPATH=`pwd`
    $ make

### Install example executor in HDFS

    $ hadoop fs -mkdir /go-tmp
    $ hadoop fs -put ./bin/example_executor /go-tmp

### Run example framework:

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

