package events

import (
	"strings"

	xmetrics "github.com/mesos/mesos-go/api/v1/lib/extras/metrics"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func Metrics(harness xmetrics.Harness) Decorator {
	return func(h Handler) Handler {
		if h == nil {
			return h
		}
		return HandlerFunc(func(e *scheduler.Event) error {
			typename := strings.ToLower(e.GetType().String())
			return harness(func() error { return h.HandleEvent(e) }, typename)
		})
	}
}
