package events

import (
	"context"

	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

type (
	// Handler is invoked upon the occurrence of some scheduler event that is generated
	// by some other component in the Mesos ecosystem (e.g. master, agent, executor, etc.)
	Handler interface {
		HandleEvent(context.Context, *scheduler.Event) error
	}

	// HandlerFunc is a functional adaptation of the Handler interface
	HandlerFunc func(context.Context, *scheduler.Event) error

	HandlerSet     map[scheduler.Event_Type]Handler
	HandlerFuncSet map[scheduler.Event_Type]HandlerFunc
)

// HandleEvent implements Handler for HandlerFunc
func (f HandlerFunc) HandleEvent(ctx context.Context, e *scheduler.Event) error { return f(ctx, e) }

func NoopHandler() HandlerFunc {
	return func(_ context.Context, _ *scheduler.Event) error { return nil }
}

// HandleEvent implements Handler for HandlerSet
func (hs HandlerSet) HandleEvent(ctx context.Context, e *scheduler.Event) (err error) {
	if h := hs[e.GetType()]; h != nil {
		return h.HandleEvent(ctx, e)
	}
	return nil
}

// HandleEvent implements Handler for HandlerFuncSet
func (hs HandlerFuncSet) HandleEvent(ctx context.Context, e *scheduler.Event) (err error) {
	if h := hs[e.GetType()]; h != nil {
		return h.HandleEvent(ctx, e)
	}
	return nil
}
