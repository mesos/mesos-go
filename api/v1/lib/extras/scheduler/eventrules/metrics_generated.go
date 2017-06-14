package eventrules

// go generate -import github.com/mesos/mesos-go/api/v1/lib/scheduler -type E:*scheduler.Event:&scheduler.Event{} -output metrics_generated.go
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"
	"strings"

	"github.com/mesos/mesos-go/api/v1/lib/extras/metrics"

	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func Metrics(harness metrics.Harness) Rule {
	return func(ctx context.Context, e *scheduler.Event, err error, ch Chain) (context.Context, *scheduler.Event, error) {
		typename := strings.ToLower(e.GetType().String())
		harness(func() error {
			ctx, e, err = ch(ctx, e, err)
			return err
		}, typename)
		return ctx, e, err
	}
}
