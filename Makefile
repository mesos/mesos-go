PROTO_PATH := ${GOPATH}/src/:./vendor/:.
PROTO_PATH := ${PROTO_PATH}:./vendor/github.com/gogo/protobuf/protobuf
PROTO_PATH := ${PROTO_PATH}:./vendor/github.com/gogo/protobuf/gogoproto

PACKAGES ?= $(shell go list ./...|grep -v vendor)
BINARIES ?= $(shell go list -f "{{.Name}} {{.ImportPath}}" ./cmd/...|grep -v -e vendor|grep -e ^main|cut -f2 -d' ')
TEST_FLAGS ?= -v -race

.PHONY: all
all: test

.PHONY: install
install:
	go install $(BINARIES)

.PHONY: test
test:
	go $@ $(TEST_FLAGS) $(PACKAGES)

.PHONY: vet
vet:
	go $@ $(PACKAGES)

.PHONY: codecs
codecs: protobufs ffjson

.PHONY: protobufs
protobufs: clean-protobufs
	protoc --proto_path="${PROTO_PATH}" --gogo_out=. *.proto
	protoc --proto_path="${PROTO_PATH}" --gogo_out=. ./scheduler/*.proto
	protoc --proto_path="${PROTO_PATH}" --gogo_out=. ./executor/*.proto

.PHONY: clean-protobufs
clean-protobufs:
	-rm *.pb.go **/*.pb.go

.PHONY: ffjson
ffjson: clean-ffjson
	ffjson *.pb.go
	ffjson scheduler/*.pb.go
	ffjson executor/*.pb.go

.PHONY: clean-ffjson
clean-ffjson:
	rm -f *ffjson.go **/*ffjson.go

GOPKG		:= github.com/mesos/mesos-go
GOPKG_DIRNAME	:= $(shell dirname $(GOPKG))
UID		?= $(shell id -u $$USER)
GID		?= $(shell id -g $$USER)

DOCKER_IMAGE_REPO       ?= jdef
DOCKER_IMAGE_NAME       ?= example-scheduler-httpv1
DOCKER_IMAGE_VERSION    ?= latest
DOCKER_IMAGE_TAG        ?= $(DOCKER_IMAGE_REPO)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_VERSION)

GOLDFLAGS	= -X $(GOPKG)/cmd.DockerImageTag=$(DOCKER_IMAGE_TAG)

BUILD_STEP	:= 'ln -s /src /go/src/$(GOPKG) && cd /go/src/$(GOPKG) && go install -ldflags "$(GOLDFLAGS)" $(BINARIES)'
COPY_STEP	:= 'cp /go/bin/* /src/_output/ && chown $(UID):$(GID) /src/_output/*'

# required for docker/Makefile
export DOCKER_IMAGE_TAG

.PHONY: docker
docker:
	mkdir -p _output
	test -n "$(UID)" || (echo 'ERROR: $$UID is undefined'; exit 1)
	test -n "$(GID)" || (echo 'ERROR: $$GID is undefined'; exit 1)
	docker run --rm -v "$$PWD":/src -w /go/src/$(GOPKG_DIRNAME) golang:1.6.1-alpine sh -c $(BUILD_STEP)' && '$(COPY_STEP)
	make -C docker
