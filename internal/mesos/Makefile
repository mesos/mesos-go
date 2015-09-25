PROTO_PATH := .:./vendor/src/
PROTO_PATH := ${PROTO_PATH}:./vendor/src/github.com/gogo/protobuf/protobuf
PROTO_PATH := ${PROTO_PATH}:./vendor/src/github.com/gogo/protobuf/gogoproto

all: *.proto
	protoc --proto_path="${PROTO_PATH}" --gogo_out=. *.proto

clean: *.go
	rm *.go
