package controller

import (
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
)

type (
	// Option modifies a Config, returns an Option that acts as an "undo"
	Option func(*Config) Option

	// Config is a controller configuration. Public fields are REQUIRED. Optional properties are
	// configured by applying Option funcs.
	Config struct {
		doneFunc               func() bool
		frameworkIDFunc        func() string
		handler                events.Handler
		registrationTokens     <-chan struct{}
		subscriptionTerminated func(error)
	}
)

// WithEventHandler sets the consumer of scheduler events. The controller's internal event processing
// loop is aborted if a Handler returns a non-nil error, after which the controller may attempt
// to re-register (subscribe) with Mesos.
func WithEventHandler(handler events.Handler, ds ...events.Decorator) Option {
	return func(c *Config) Option {
		old := c.handler
		c.handler = events.Decorators(ds).Apply(handler)
		return WithEventHandler(old)
	}
}

// WithFrameworkID sets a fetcher for the current Mesos-assigned framework ID. Frameworks are expected to
// track this ID (that comes from Mesos, in a SUBSCRIBED event).
// frameworkIDFunc is optional; nil tells the controller to always register as a new framework
// for each subscription attempt.
func WithFrameworkID(frameworkIDFunc func() string) Option {
	return func(c *Config) Option {
		old := c.frameworkIDFunc
		c.frameworkIDFunc = frameworkIDFunc
		return WithFrameworkID(old)
	}
}

// WithDone sets a fetcher func that returns true when the controller should exit.
// doneFunc is optional; nil equates to a func that always returns false.
func WithDone(doneFunc func() bool) Option {
	return func(c *Config) Option {
		old := c.doneFunc
		c.doneFunc = doneFunc
		return WithDone(old)
	}
}

// WithSubscriptionTerminated sets a handler that is invoked at the end of every subscription cycle; the
// given error may be nil if no error occurred. subscriptionTerminated is optional; if nil then errors are
// swallowed.
func WithSubscriptionTerminated(handler func(error)) Option {
	return func(c *Config) Option {
		old := c.subscriptionTerminated
		c.subscriptionTerminated = handler
		return WithSubscriptionTerminated(old)
	}
}

// WithRegistrationTokens limits the rate at which a framework (re)registers with Mesos.
// The chan should either be non-blocking, or should yield a struct{} in order to allow the
// framework registration process to continue. May be nil.
func WithRegistrationTokens(registrationTokens <-chan struct{}) Option {
	return func(c *Config) Option {
		old := c.registrationTokens
		c.registrationTokens = registrationTokens
		return WithRegistrationTokens(old)
	}
}

func (c *Config) tryFrameworkID() (result string) {
	if c.frameworkIDFunc != nil {
		result = c.frameworkIDFunc()
	}
	return
}

func (c *Config) tryDone() (result bool) { return c.doneFunc != nil && c.doneFunc() }

// Run executes a control loop that registers a framework with Mesos and processes the scheduler events
// that flow through the subscription. Upon disconnection, if the current configuration reports "not done"
// then the controller will attempt to re-register the framework and continue processing events.
func Run(framework *mesos.FrameworkInfo, caller calls.Caller, options ...Option) (lastErr error) {
	var config Config
	for _, opt := range options {
		if opt != nil {
			opt(&config)
		}
	}
	if config.handler == nil {
		config.handler = DefaultHandler
	}
	subscribe := calls.Subscribe(framework)
	for !config.tryDone() {
		frameworkID := config.tryFrameworkID()
		if framework.GetFailoverTimeout() > 0 && frameworkID != "" {
			subscribe.With(calls.SubscribeTo(frameworkID))
		}
		if config.registrationTokens != nil {
			<-config.registrationTokens
		}
		resp, err := caller.Call(subscribe)
		lastErr = processSubscription(config, resp, err)
		if config.subscriptionTerminated != nil {
			config.subscriptionTerminated(lastErr)
		}
	}
	return
}

func processSubscription(config Config, resp mesos.Response, err error) error {
	if resp != nil {
		defer resp.Close()
	}
	if err == nil {
		err = eventLoop(config, resp)
	}
	return err
}

// eventLoop returns the framework ID received by mesos (if any); callers should check for a
// framework ID regardless of whether error != nil.
func eventLoop(config Config, eventDecoder encoding.Decoder) (err error) {
	for err == nil && !config.tryDone() {
		var e scheduler.Event
		if err = eventDecoder.Decode(&e); err == nil {
			err = config.handler.HandleEvent(&e)
		}
	}
	return err
}

// DefaultHandler defaults to events.NoopHandler
var DefaultHandler = events.NoopHandler()
