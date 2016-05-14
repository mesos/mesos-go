package httpsched

import (
	"strings"

	"github.com/mesos/mesos-go"
	xmetrics "github.com/mesos/mesos-go/extras/metrics"
	"github.com/mesos/mesos-go/scheduler"
)

func CallerMetrics(harness xmetrics.Harness) Decorator {
	return func(caller Caller) (metricsCaller Caller) {
		if caller != nil {
			metricsCaller = &CallerAdapter{
				CallFunc: func(c *scheduler.Call) (res mesos.Response, caller2 Caller, err error) {
					typename := strings.ToLower(c.GetType().String())
					harness(func() error {
						res, caller2, err = caller.Call(c)
						return err // need to count these
					}, typename)
					return
				},
			}
		}
		return
	}
}
