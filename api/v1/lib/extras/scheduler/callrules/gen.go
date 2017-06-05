package callrules

//go:generate go run ../../gen/rules.go ../../gen/gen.go -import github.com/mesos/mesos-go/api/v1/lib -import github.com/mesos/mesos-go/api/v1/lib/scheduler -type E:*scheduler.Call:&scheduler.Call{} -type Z:mesos.Response:&mesos.ResponseWrapper{}

//go:generate go run ../../gen/rule_metrics.go ../../gen/gen.go -import github.com/mesos/mesos-go/api/v1/lib -import github.com/mesos/mesos-go/api/v1/lib/scheduler -type E:*scheduler.Call -type Z:mesos.Response -output metrics_generated.go
