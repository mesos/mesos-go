EXAMPLES = examples

all: go-clean pkg-build-install test-framework test-executor

go-clean:
	go clean

pkg-build-install:
	go install -v \
	./detector \
	./executor \
	./healthchecker \
	./mesosproto \
	./mesosutil \
	./messenger \
	./scheduler \
	./upid

test-framework: $(PKGS)
	rm -rf ${EXAMPLES}/$@
	go build -o ${EXAMPLES}/$@ ${EXAMPLES}/test_framework.go 

test-executor: $(PKGS)
	rm -rf ${EXAMPLES}/$@
	go build -o ${EXAMPLES}/$@ ${EXAMPLES}/test_executor.go 
