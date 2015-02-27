EXAMPLES = examples

PKG_PREFIX := github.com/mesos/mesos-go
LIBS :=	\
	auth \
	auth/callback \
	auth/sasl \
	auth/sasl/mech \
	auth/sasl/mech/crammd5 \
	detector \
	detector/zoo \
	executor \
	healthchecker \
	mesosproto \
	mesosutil \
	mesosutil/process \
	messenger \
	scheduler \
	upid

.PHONY: format all go-clean pkg-build-install example-scheduler example-executor test test.v

all: go-clean pkg-build-install examples

go-clean:
	go clean

pkg-build-install:
	go install -v ${LIBS:%=./%}

examples: example-scheduler example-executor

example-scheduler:
	rm -rf ${EXAMPLES}/$@
	go build -o ${EXAMPLES}/$@ ${EXAMPLES}/example_scheduler.go

example-executor:
	rm -rf ${EXAMPLES}/$@
	go build -o ${EXAMPLES}/$@ ${EXAMPLES}/example_executor.go

format:
	go fmt ${LIBS:%=$(PKG_PREFIX)/%}

vet:
	go vet ${LIBS:%=$(PKG_PREFIX)/%}

test test.v:
	flags=""; test "$@" != "test.v" || flags="-test.v"; pkg="${TEST}"; test -n "$$pkg" || pkg="${LIBS:%=$(PKG_PREFIX)/%}"; go test $$pkg $$flags
