package events

//go:generate go run ../../extras/gen/handlers.go ../../extras/gen/gen.go -import github.com/mesos/mesos-go/api/v1/lib/scheduler -event_type *scheduler.Event -type ET:scheduler.Event_Type
