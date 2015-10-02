PROTO_PATH := ${GOPATH}/src/:./vendor/src/:.
PROTO_PATH := ${PROTO_PATH}:./vendor/src/github.com/gogo/protobuf/protobuf
PROTO_PATH := ${PROTO_PATH}:./vendor/src/github.com/gogo/protobuf/gogoproto

.PHONY: test protobufs

all: test

test:
	go test ./...

protobufs:
	protoc --proto_path="${PROTO_PATH}" --gogo_out=. *.proto
	protoc --proto_path="${PROTO_PATH}" --gogo_out=. ./scheduler/*.proto

