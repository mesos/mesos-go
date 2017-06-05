package eventrules

//go:generate go run ../../gen/rules.go ../../gen/gen.go -import github.com/mesos/mesos-go/api/v1/lib/scheduler -type E:*scheduler.Event:&scheduler.Event{}
