package callrules

import (
	"context"
	"strings"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/extras/metrics"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func Metrics(harness metrics.Harness) Rule {
	return func(ctx context.Context, c *scheduler.Call, r mesos.Response, err error, ch Chain) (context.Context, *scheduler.Call, mesos.Response, error) {
		typename := strings.ToLower(c.GetType().String())
		harness(func() error {
			_, _, r, err = ch(ctx, c, r, err)
			return err // need to count these
		}, typename)
		return ctx, c, r, err
	}
}
