package scheduler

// A CallOpt is a functional option type for Calls.
type CallOpt func(*Call)

// With applies the given CallOpts to the receiving Call, returning it.
func (c *Call) With(opts ...CallOpt) *Call {
	for _, opt := range opts {
		opt(c)
	}
	return c
}
