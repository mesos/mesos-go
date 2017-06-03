package callrules

//go:generate go run ../../gen/rules.go ../../gen/gen.go -import github.com/mesos/mesos-go/api/v1/lib -import github.com/mesos/mesos-go/api/v1/lib/scheduler -event_type *scheduler.Call -return_type mesos.Response -return_prototype &mesos.ResponseWrapper{}
