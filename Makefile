all: examples

.PHONY : clean c-bridge check_mesos check_proto_headers
c-bridge:
	cd c-bridge; make all

check_mesos:
	@if [ ! -f /usr/local/lib/libmesos.so ]; then \
  	echo "Error: Expecting libmesos.so in /usr/local/lib"; exit 2; \
	else true; fi
	@if [ ! -d /usr/local/include/mesos ]; then \
  	echo "Error: Expecting mesos headers in /usr/local/include/mesos"; exit 2; \
	else true; fi

check_proto_headers:
	@cd c-bridge;g++ test_headers.cpp 2> /dev/null; if [ $$? -ne 0 ]; then\
  	echo "Error: Expected installed protocol buffer"; exit 2; \
	else true; fi

protos:
	go get code.google.com/p/goprotobuf/proto
	go get code.google.com/p/goprotobuf/protoc-gen-go

examples: check_proto_headers check_mesos protos c-bridge
	go install github.com/mesosphere/mesos-go/src/mesos.apache.org/example_framework
	go install github.com/mesosphere/mesos-go/src/mesos.apache.org/example_executor

clean:
	@cd c-bridge; make clean
	go clean
