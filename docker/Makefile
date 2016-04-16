all:
	mkdir -p _output
	cp Dockerfile _output/
	cp $$GOPATH/bin/example-scheduler _output/
	cp $$HOME/mesos-go-examples/executor _output/
	docker build -t jdef/example-scheduler _output
