package events

import (
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

type (
	// Decorator functions typically modify behavior of the given delegate Handler.
	Decorator func(Handler) Handler

	// Decorators aggregates Decorator functions
	Decorators []Decorator
)

var noopDecorator = Decorator(func(h Handler) Handler { return h })

// If returns the receiving Decorator if the given bool is true; otherwise returns a no-op
// Decorator instance.
func (d Decorator) If(b bool) Decorator {
	if d == nil {
		return noopDecorator
	}
	result := noopDecorator
	if b {
		result = d
	}
	return result
}

// When returns a Decorator that evaluates the bool func every time the Handler is invoked.
// When f returns true, the Decorated Handler is invoked, otherwise the original Handler is.
func (d Decorator) When(f func() bool) Decorator {
	if d == nil || f == nil {
		return noopDecorator
	}
	return func(h Handler) Handler {
		// generates a new decorator every time the Decorator func is invoked.
		// probably OK for now.
		decorated := d(h)
		return HandlerFunc(func(e *scheduler.Event) (err error) {
			if f() {
				// let the decorated handler process this
				err = decorated.HandleEvent(e)
			} else {
				err = h.HandleEvent(e)
			}
			return
		})
	}
}

// Apply applies the Decorators in the order they're listed such that the last Decorator invoked
// generates the final (wrapping) Handler that is ultimately returned.
func (ds Decorators) Apply(h Handler) Handler {
	for _, d := range ds {
		h = d.Apply(h)
	}
	return h
}

func (d Decorator) Apply(h Handler) Handler {
	if d != nil {
		h = d(h)
	}
	return h
}
