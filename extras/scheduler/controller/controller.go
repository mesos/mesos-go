package controller

import (
	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli/httpsched"
	"github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/scheduler/calls"
	"github.com/mesos/mesos-go/scheduler/events"
)

type (
	Context interface {
		// Done returns true when the controller should exit
		Done() bool

		// FrameworkID returns the current Mesos-assigned framework ID. Frameworks are expected to
		// track this ID (that comes from Mesos, in a SUBSCRIBED event).
		FrameworkID() string

		// Error is an error handler that is invoked at the end of every subscription cycle; the given
		// error may be nil (if no errors occurred).
		Error(error)
	}

	ContextAdapter struct {
		// FrameworkIDFunc is optional; nil tells the controller to always register as a new framework
		// for each subscription attempt.
		FrameworkIDFunc func() string

		// Done is optional; nil equates to a func that always returns false
		DoneFunc func() bool

		// ErrorFunc is optional; if nil then errors are swallowed
		ErrorFunc func(error)
	}

	Controller struct {
		Context       Context              // Context is required
		Framework     *mesos.FrameworkInfo // FrameworkInfo is required
		InitialCaller httpsched.Caller     // InitialCaller is required
		Handler       events.Handler       // Handler (optional) processes scheduler events

		// RegistrationTokens (optional) limits the rate at which a framework (re)registers with Mesos.
		// The returned chan should either be non-blocking (nil/closed), or should yield a struct{} in
		// order to allow the framework registration process to continue. May be nil.
		RegistrationTokens <-chan struct{}

		// Caller (optional) indicates a change of caller; the decorator returns the caller that will be
		// used by the controller going forward until the next change-of-caller. May be nil.
		Caller httpsched.Decorator
	}
)

// Run executes a control loop that registers a framework with Mesos and processes the scheduler events
// that flow through the subscription. Upon disconnection, if the given Context reports !Done() then the
// controller will attempt to re-register the framework and continue processing events.
func (control *Controller) Run() (lastErr error) {
	subscribe := calls.Subscribe(true, control.Framework)
	for !control.Context.Done() {
		var (
			caller      = control.Caller.Apply(control.InitialCaller)
			frameworkID = control.Context.FrameworkID()
		)
		if control.Framework.GetFailoverTimeout() > 0 && frameworkID != "" {
			subscribe.Subscribe.FrameworkInfo.ID = &mesos.FrameworkID{Value: frameworkID}
		}
		<-control.RegistrationTokens
		resp, subscribedCaller, err := caller.Call(subscribe)
		if subscribedCaller != nil {
			control.Caller.Apply(subscribedCaller)
		}
		lastErr = control.processSubscription(resp, err)
		control.Context.Error(lastErr)
	}
	return
}

func (control *Controller) processSubscription(resp mesos.Response, err error) error {
	if resp != nil {
		defer resp.Close()
	}
	if err == nil {
		err = control.eventLoop(resp.Decoder())
	}
	return err
}

// eventLoop returns the framework ID received by mesos (if any); callers should check for a
// framework ID regardless of whether error != nil.
func (control *Controller) eventLoop(eventDecoder encoding.Decoder) (err error) {
	h := control.Handler
	if h == nil {
		h = events.HandlerFunc(func(*scheduler.Event) error { return nil })
	}
	for err == nil && !control.Context.Done() {
		var e scheduler.Event
		if err = eventDecoder.Invoke(&e); err == nil {
			err = h.HandleEvent(&e)
		}
	}
	return err
}

var _ = Context(&ContextAdapter{}) // ContextAdapter implements Context

func (ca *ContextAdapter) Done() bool {
	return ca.DoneFunc != nil && ca.DoneFunc()
}
func (ca *ContextAdapter) FrameworkID() (id string) {
	if ca.FrameworkIDFunc != nil {
		id = ca.FrameworkIDFunc()
	}
	return
}
func (ca *ContextAdapter) Error(err error) {
	if ca.ErrorFunc != nil {
		ca.ErrorFunc(err)
	}
}
