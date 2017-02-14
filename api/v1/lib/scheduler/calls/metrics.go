package calls

import (
	"strings"

	"github.com/mesos/mesos-go/api/v1/lib"
	xmetrics "github.com/mesos/mesos-go/api/v1/lib/extras/metrics"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func CallerMetrics(harness xmetrics.Harness) Decorator {
	return func(caller Caller) (metricsCaller Caller) {
		if caller != nil {
			metricsCaller = CallerFunc(func(c *scheduler.Call) (res mesos.Response, err error) {
				typename := strings.ToLower(c.GetType().String())
				harness(func() error {
					res, err = caller.Call(c)
					return err // need to count these
				}, typename)
				return
			})
		}
		return
	}
}
