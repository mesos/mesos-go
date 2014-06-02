all: examples

.PHONY : clean check_mesos
check_mesos:
	@if [ ! -f /usr/local/lib/libmesos.so -a ! -f /usr/local/lib/libmesos.dylib ]; then \
		echo "Error: Expecting libmesos.so or libmesos.dylib in /usr/local/lib"; exit 2; \
	else true; fi
	@if [ ! -d /usr/local/include/mesos ]; then \
		echo "Error: Expecting mesos headers in /usr/local/include/mesos"; exit 2; \
	else true; fi

protos:
	go get code.google.com/p/goprotobuf/proto
	go get code.google.com/p/goprotobuf/protoc-gen-go

examples: check_mesos protos
	go install ./example_framework
	go install ./example_executor

clean:
	go clean
