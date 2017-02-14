package controller

import (
	"fmt"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
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

	Config struct {
		Context   Context              // Context is required
		Framework *mesos.FrameworkInfo // FrameworkInfo is required
		Caller    calls.Caller         // Caller  is required

		// Handler (optional) processes scheduler events. The controller's internal event processing
		// loop is aborted if a Handler returns a non-nil error, after which the controller may attempt
		// to re-register (subscribe) with Mesos.
		Handler events.Handler

		// RegistrationTokens (optional) limits the rate at which a framework (re)registers with Mesos.
		// The returned chan should either be non-blocking (nil/closed), or should yield a struct{} in
		// order to allow the framework registration process to continue. May be nil.
		RegistrationTokens <-chan struct{}
	}

	Controller interface {
		// Run executes the controller using the given Config
		Run(Config) error
	}

	// ControllerFunc is a functional adaptation of a Controller
	ControllerFunc func(Config) error

	controllerImpl int
)

// Run implements Controller for ControllerFunc
func (cf ControllerFunc) Run(config Config) error { return cf(config) }

func New() Controller {
	return new(controllerImpl)
}

// Run executes a control loop that registers a framework with Mesos and processes the scheduler events
// that flow through the subscription. Upon disconnection, if the given Context reports !Done() then the
// controller will attempt to re-register the framework and continue processing events.
func (_ *controllerImpl) Run(config Config) (lastErr error) {
	subscribe := calls.Subscribe(config.Framework)
	for !config.Context.Done() {
		frameworkID := config.Context.FrameworkID()
		if config.Framework.GetFailoverTimeout() > 0 && frameworkID != "" {
			subscribe.Subscribe.FrameworkInfo.ID = &mesos.FrameworkID{Value: frameworkID}
		}
		<-config.RegistrationTokens
		resp, err := config.Caller.Call(subscribe)
		lastErr = processSubscription(config, resp, err)
		config.Context.Error(lastErr)
	}
	return
}

func processSubscription(config Config, resp mesos.Response, err error) error {
	if resp != nil {
		defer resp.Close()
	}
	if err == nil {
		err = eventLoop(config, resp.Decoder())
	}
	return err
}

// eventLoop returns the framework ID received by mesos (if any); callers should check for a
// framework ID regardless of whether error != nil.
func eventLoop(config Config, eventDecoder encoding.Decoder) (err error) {
	h := config.Handler
	if h == nil {
		h = events.HandlerFunc(DefaultHandler)
	}
	for err == nil && !config.Context.Done() {
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

// DefaultHandler provides the minimum implementation required for correct controller behavior.
func DefaultHandler(e *scheduler.Event) (err error) {
	if e.GetType() == scheduler.Event_ERROR {
		// it's recommended that we abort and re-try subscribing; returning an
		// error here will cause the event loop to terminate and the connection
		// will be reset.
		err = fmt.Errorf("ERROR: %q", e.GetError().GetMessage())
	}
	return
}
