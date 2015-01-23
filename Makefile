EXAMPLES = examples

PKG_PREFIX := github.com/mesos/mesos-go
LIBS :=	\
	auth \
	auth/callback \
	auth/sasl \
	auth/sasl/mech \
	auth/sasl/mech/crammd5 \
	detector \
	executor \
	healthchecker \
	mesosproto \
	mesosutil \
	messenger \
	scheduler \
	upid

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

test:
	go test ${LIBS:%=$(PKG_PREFIX)/%}
