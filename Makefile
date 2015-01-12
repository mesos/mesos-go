EXAMPLES = examples

PKG_PREFIX := github.com/mesos/mesos-go
LIBS :=	detector \
	executor \
	healthchecker \
	mesosproto \
	mesosutil \
	messenger \
	scheduler \
	upid \
	auth \
	auth/crammd5

.PHONY: format all go-clean pkg-build-install test-framework test-executor

all: go-clean pkg-build-install test-framework test-executor

go-clean:
	go clean

pkg-build-install:
	go install -v ${LIBS:%=./%}

test-framework:
	rm -rf ${EXAMPLES}/$@
	go build -o ${EXAMPLES}/$@ ${EXAMPLES}/test_framework.go 

test-executor:
	rm -rf ${EXAMPLES}/$@
	go build -o ${EXAMPLES}/$@ ${EXAMPLES}/test_executor.go 

format:
	go fmt ${LIBS:%=$(PKG_PREFIX)/%}
