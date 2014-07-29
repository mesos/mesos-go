all: *.proto
	protoc --proto_path=${GOPATH}/src:${GOPATH}/src/code.google.com/p/gogoprotobuf/protobuf:. --gogo_out=. *.proto
