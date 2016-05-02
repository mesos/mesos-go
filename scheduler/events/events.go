package events

import (
	"github.com/mesos/mesos-go/scheduler"
)

type (
	// Handler is invoked upon the occurrence of some scheduler event that is generated
	// by some other component in the Mesos ecosystem (e.g. master, agent, executor, etc.)
	Handler interface {
		HandleEvent(*scheduler.Event) error
	}

	// HandlerFunc is a functional adaptation of the Handler interface
	HandlerFunc func(*scheduler.Event) error

	// Mux maps event types to Handlers (only one Handler for each type). A "default"
	// Handler implementation may be provided to handle cases in which there is no
	// registered Handler for specific event type.
	Mux struct {
		handlers       map[scheduler.Event_Type]Handler
		defaultHandler Handler
	}

	// Option is a functional configuration option that returns an "undo" option that
	// reverts the change made by the option.
	Option func(*Mux) Option

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
		if d != nil {
			h = d(h)
		}
	}
	return h
}

// HandleEvents implements Handler for HandlerFunc
func (f HandlerFunc) HandleEvent(e *scheduler.Event) error { return f(e) }

// NewMux generates and returns a new, empty Mux instance.
func NewMux(opts ...Option) *Mux {
	m := &Mux{
		handlers: make(map[scheduler.Event_Type]Handler),
	}
	m.With(opts...)
	return m
}

// With applies the given options to the Mux and returns the result of invoking
// the last Option func. If no options are provided then a no-op Option is returned.
func (m *Mux) With(opts ...Option) Option {
	var last Option // defaults to noop
	last = Option(func(x *Mux) Option { return last })

	for _, o := range opts {
		if o != nil {
			last = o(m)
		}
	}
	return last
}

// HandleEvent implements Handler for Mux
func (m *Mux) HandleEvent(e *scheduler.Event) (err error) {
	h, found := m.handlers[e.GetType()]
	if !found {
		h = m.defaultHandler
	}
	if h != nil {
		err = h.HandleEvent(e)
	}
	return
}

// Handle returns an option that configures a Handler to handle a specific event type.
// If the specified Handler is nil then any currently registered Handler for the given
// event type is deleted upon application of the returned Option.
func Handle(et scheduler.Event_Type, eh Handler) Option {
	return func(m *Mux) Option {
		old := m.handlers[et]
		if eh == nil {
			delete(m.handlers, et)
		} else {
			m.handlers[et] = eh
		}
		return Handle(et, old)
	}
}

// DefaultHandler returns an option that configures the default handler that's invoked
// in cases where there is no Handler registered for specific event type.
func DefaultHandler(eh Handler) Option {
	return func(m *Mux) Option {
		old := m.defaultHandler
		m.defaultHandler = eh
		return DefaultHandler(old)
	}
}
