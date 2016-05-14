package controller

import (
	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli/httpsched"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/scheduler/calls"
	"github.com/mesos/mesos-go/scheduler/events"
)

type Context interface {
	// Handler returns a scheduler event processor
	Handler() events.Handler

	// Done returns true when the controller should exit
	Done() bool

	// FrameworkID returns the current Mesos-assigned framework ID
	FrameworkID() string

	// RegistrationTokens limits the rate at which a framework (re)registers with Mesos. The
	// returned chan should either be closed (non-blocking), or should yield a struct{} in order
	// to allow the framework registration process to continue.
	RegistrationTokens() <-chan struct{}

	// Caller indicates a change of caller; a context optionally decorate's the given caller, returns
	// the caller that will be used going forward.
	Caller(httpsched.Caller) httpsched.Caller

	// Error is an error handler that is invoked at the end of every subscription cycle; the given
	// error may be nil (if no errors occurred).
	Error(error)
}

// Run executes a control loop that registers a framework with Mesos and processes the scheduler events
// that flow through the subscription. Upon disconnection, if the given Context reports !Done() then the
// controller will attempt to re-register the framework and continue processing events.
func Run(ctx Context, frameworkInfo *mesos.FrameworkInfo, userCaller httpsched.Caller) (lastErr error) {
	subscribe := calls.Subscribe(true, frameworkInfo)
	for !ctx.Done() {
		caller := ctx.Caller(userCaller)
		if frameworkID := ctx.FrameworkID(); frameworkInfo.GetFailoverTimeout() > 0 && frameworkID != "" {
			subscribe.Subscribe.FrameworkInfo.ID = &mesos.FrameworkID{Value: frameworkID}
		}
		<-ctx.RegistrationTokens()
		resp, subscribedCaller, err := caller.Call(subscribe)
		if subscribedCaller != nil {
			ctx.Caller(subscribedCaller)
		}
		lastErr = processSubscription(ctx, resp, err)
		ctx.Error(lastErr)
	}
	return
}

func processSubscription(
	ctx Context,
	resp mesos.Response,
	err error,
) error {
	if resp != nil {
		defer resp.Close()
	}
	if err == nil {
		err = eventLoop(ctx, resp.Decoder())
	}
	return err
}

// eventLoop returns the framework ID received by mesos (if any); callers should check for a
// framework ID regardless of whether error != nil.
func eventLoop(ctx Context, eventDecoder encoding.Decoder) (err error) {
	for err == nil && !ctx.Done() {
		var e scheduler.Event
		if err = eventDecoder.Invoke(&e); err == nil {
			if h := ctx.Handler(); h != nil {
				err = h.HandleEvent(&e)
			}
		}
	}
	return err
}
