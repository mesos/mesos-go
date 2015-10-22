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

// SubscribeCall returns a subscribe call with the given parameters.
func SubscribeCall(id string, force bool, info *mesos.FrameworkInfo) *Call {
	return &Call{
		Type:        Call_SUBSCRIBE.Enum(),
		FrameworkID: &mesos.FrameworkID{Value: id},
		Subscribe:   &Call_Subscribe{FrameworkInfo: info, Force: force},
	}
}
