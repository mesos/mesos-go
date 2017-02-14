package events

import (
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

type PredicateBool func() bool

func (b PredicateBool) Happens() scheduler.EventPredicate {
	return func(_ *scheduler.Event) bool { return b() }
}
