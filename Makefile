PROTO_PATH := ${GOPATH}/src/:./vendor/:.
PROTO_PATH := ${PROTO_PATH}:./vendor/github.com/gogo/protobuf/protobuf
PROTO_PATH := ${PROTO_PATH}:./vendor/github.com/gogo/protobuf/gogoproto

PACKAGES ?= $(shell go list ./...|grep -v vendor)
TEST_FLAGS ?= -v -race

.PHONY: all
all: test

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

.PHONY: clean-protobufs
clean-protobufs:
	-rm *.pb.go **/*.pb.go

.PHONY: ffjson
ffjson: clean-ffjson
	ffjson *.pb.go
	ffjson **/*.pb.go

.PHONY: clean-ffjson
clean-ffjson:
	-rm *ffjson.go **/*ffjson.go
