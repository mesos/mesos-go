package events

//go:generate go run ../../extras/gen/handlers.go ../../extras/gen/gen.go -import github.com/mesos/mesos-go/api/v1/lib/executor -event_type *executor.Event -type ET:executor.Event_Type
