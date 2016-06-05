package events

import (
	"github.com/mesos/mesos-go/scheduler"
)

type PredicateBool func() bool

func (b PredicateBool) Happens() scheduler.EventPredicate {
	return func(_ *scheduler.Event) bool { return b() }
}
