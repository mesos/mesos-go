package callrules

// go generate -import github.com/mesos/mesos-go/api/v1/lib -import github.com/mesos/mesos-go/api/v1/lib/scheduler -type E:*scheduler.Call:&scheduler.Call{} -type Z:mesos.Response:&mesos.ResponseWrapper{} -output metrics_generated.go
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"
	"strings"

	"github.com/mesos/mesos-go/api/v1/lib/extras/metrics"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func Metrics(harness metrics.Harness) Rule {
	return func(ctx context.Context, e *scheduler.Call, z mesos.Response, err error, ch Chain) (context.Context, *scheduler.Call, mesos.Response, error) {
		typename := strings.ToLower(e.GetType().String())
		harness(func() error {
			ctx, e, z, err = ch(ctx, e, z, err)
			return err
		}, typename)
		return ctx, e, z, err
	}
}
