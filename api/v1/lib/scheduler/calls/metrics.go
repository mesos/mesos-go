package calls

// TODO(jdef): move this code to the extras tree, it doesn't belong here

import (
	"context"
	"strings"

	"github.com/mesos/mesos-go/api/v1/lib"
	xmetrics "github.com/mesos/mesos-go/api/v1/lib/extras/metrics"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func CallerMetrics(harness xmetrics.Harness) Decorator {
	return func(caller Caller) (metricsCaller Caller) {
		if caller != nil {
			metricsCaller = CallerFunc(func(ctx context.Context, c *scheduler.Call) (res mesos.Response, err error) {
				typename := strings.ToLower(c.GetType().String())
				harness(func() error {
					res, err = caller.Call(ctx, c)
					return err // need to count these
				}, typename)
				return
			})
		}
		return
	}
}
