FROM ubuntu:14.04
MAINTAINER James DeFelice <james@mesosphere.com>

workdir	/opt
add	example-scheduler ./
add	executor ./

cmd []
entrypoint [ "/opt/example-scheduler", "-executor=/opt/executor", "-url=http://leader.mesos:5050/api/v1/scheduler" ]
