package eventrules

import (
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
)

// Handler generates a rule that executes the given handler.
func Handle(h events.Handler) Rule {
	if h == nil {
		return nil
	}
	return func(e *scheduler.Event, err error, chain Chain) (*scheduler.Event, error) {
		newErr := h.HandleEvent(e)
		return chain(e, Error2(err, newErr))
	}
}

// HandleF is the functional equivalent of Handle
func HandleF(h events.HandlerFunc) Rule {
	return Handle(events.Handler(h))
}

// Handler returns a rule that invokes the given Handler
func (r Rule) Handle(h events.Handler) Rule {
	return Rules{r, Handle(h)}.Eval
}

// HandleF is the functional equivalent of Handle
func (r Rule) HandleF(h events.HandlerFunc) Rule {
	return r.Handle(events.Handler(h))
}

// HandleEvent implements events.Handler for Rule
func (r Rule) HandleEvent(e *scheduler.Event) (err error) {
	if r == nil {
		return nil
	}
	_, err = r(e, nil, chainIdentity)
	return
}

// HandleEvent implements events.Handler for Rules
func (rs Rules) HandleEvent(e *scheduler.Event) error {
	return Rule(rs.Eval).HandleEvent(e)
}

/*
// Apply returns the result of a singleton rule set (the receiver) applied to the given event handler.
func (r Rule) Apply(h events.Handler) events.HandlerFunc {
	if r == nil {
		return h.HandleEvent
	}
	return r.Handle(h).HandleEvent
}

// ApplyF is the functional equivalent of Apply
func (r Rule) ApplyF(h events.HandlerFunc) events.HandlerFunc {
	return r.Apply(events.Handler(h))
}
*/
