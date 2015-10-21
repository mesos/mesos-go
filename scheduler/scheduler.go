package scheduler

import "github.com/mesos/mesos-go"

// A CallOpt is a functional option type for Calls.
type CallOpt func(*Call)

// With applies the given CallOpts to the receiving Call, returning it.
func (c *Call) With(opts ...CallOpt) *Call {
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Subscribe returns a CallOpt that makes a Call a Subscribe call.
func Subscribe(force bool, info mesos.FrameworkInfo) CallOpt {
	return func(c *Call) {
		t := Call_SUBSCRIBE
		c.Type = &t
		c.Subscribe = &Call_Subscribe{FrameworkInfo: &info, Force: &force}
	}
}
