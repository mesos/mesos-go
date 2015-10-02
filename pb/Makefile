PROTO_PATH := .:./vendor/src/
PROTO_PATH := ${PROTO_PATH}:./vendor/src/github.com/gogo/protobuf/protobuf
PROTO_PATH := ${PROTO_PATH}:./vendor/src/github.com/gogo/protobuf/gogoproto

all: gen test

gen:
	protoc --proto_path="${PROTO_PATH}" --gogo_out=. *.proto

test:
	go test

clean: *.go
	rm *.go
