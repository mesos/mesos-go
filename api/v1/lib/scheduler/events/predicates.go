package events

import (
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

type PredicateBool func() bool

// Predicate implements scheduler.events.Predicate
func (b PredicateBool) Predicate() scheduler.EventPredicate {
	return func(_ *scheduler.Event) bool { return b() }
}
