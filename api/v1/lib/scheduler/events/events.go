package events

import (
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
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

	// Handlers aggregates Handler things
	Handlers []Handler

	Happens interface {
		Happens() scheduler.EventPredicate
	}
)

// HandleEvent implements Handler for HandlerFunc
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

// Map returns an Option that configures multiple Handler objects.
func Map(handlers map[scheduler.Event_Type]Handler) (option Option) {
	option = func(m *Mux) Option {
		type history struct {
			et scheduler.Event_Type
			h  Handler
		}
		old := make([]history, len(handlers))
		for et, h := range handlers {
			old = append(old, history{et, m.handlers[et]})
			m.handlers[et] = h
		}
		return func(m *Mux) Option {
			for i := range old {
				if old[i].h == nil {
					delete(m.handlers, old[i].et)
				} else {
					m.handlers[old[i].et] = old[i].h
				}
			}
			return option
		}
	}
	return
}

// MapFuncs is the functional adaptation of Map
func MapFuncs(handlers map[scheduler.Event_Type]HandlerFunc) (option Option) {
	h := make(map[scheduler.Event_Type]Handler, len(handlers))
	for k, v := range handlers {
		h[k] = v
	}
	return Map(h)
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

// AcknowledgeUpdates generates a Handler that sends an Acknowledge call to Mesos for every
// UPDATE event that's received.
func AcknowledgeUpdates(callerGetter func() calls.Caller) Handler {
	return WhenFunc(scheduler.Event_UPDATE, func(e *scheduler.Event) (err error) {
		var (
			s    = e.GetUpdate().GetStatus()
			uuid = s.GetUUID()
		)
		if len(uuid) > 0 {
			ack := calls.Acknowledge(
				s.GetAgentID().GetValue(),
				s.TaskID.Value,
				uuid,
			)
			err = calls.CallNoData(callerGetter(), ack)
		}
		return
	})
}

func Once(h Handler) Handler {
	called := false
	return HandlerFunc(func(e *scheduler.Event) (err error) {
		if !called {
			called = true
			err = h.HandleEvent(e)
		}
		return
	})
}

func OnceFunc(h HandlerFunc) Handler { return Once(h) }

func When(p Happens, h Handler) Handler {
	return HandlerFunc(func(e *scheduler.Event) (err error) {
		if p.Happens().Apply(e) {
			err = h.HandleEvent(e)
		}
		return
	})
}

func WhenFunc(p Happens, h HandlerFunc) Handler { return When(p, h) }

var _ = Handler(Handlers{}) // Handlers implements Handler

// HandleEvent implements Handler for Handlers
func (hs Handlers) HandleEvent(e *scheduler.Event) (err error) {
	for _, h := range hs {
		if h != nil {
			if err = h.HandleEvent(e); err != nil {
				break
			}
		}
	}
	return err
}
