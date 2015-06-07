EXAMPLES = examples

.PHONY: format all go-clean pkg-build-install example-scheduler example-executor test

all: go-clean pkg-build-install examples

go-clean:
	go clean

pkg-build-install:
	go install -v ./...

examples: example-scheduler example-executor

example-scheduler:
	rm -rf ${EXAMPLES}/$@
	go build -o ${EXAMPLES}/$@ ${EXAMPLES}/example_scheduler.go

example-executor:
	rm -rf ${EXAMPLES}/$@
	go build -o ${EXAMPLES}/$@ ${EXAMPLES}/example_executor.go

format:
	gofmt -s -w .

formatted:
	! gofmt -s -d . 2>&1 | read

vet:
	go vet ./...

test:
	go test -race ./...

check: formatted vet test
