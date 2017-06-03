package callrules

import (
	"context"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
)

// Call returns a Rule that invokes the given Caller
func Call(caller calls.Caller) Rule {
	if caller == nil {
		return nil
	}
	return func(ctx context.Context, c *scheduler.Call, _ mesos.Response, _ error, ch Chain) (context.Context, *scheduler.Call, mesos.Response, error) {
		resp, err := caller.Call(ctx, c)
		return ch(ctx, c, resp, err)
	}
}

// CallF returns a Rule that invokes the given CallerFunc
func CallF(cf calls.CallerFunc) Rule {
	return Call(calls.Caller(cf))
}

// Caller returns a Rule that invokes the receiver and then calls the given Caller
func (r Rule) Caller(caller calls.Caller) Rule {
	return Rules{r, Call(caller)}.Eval
}

// CallerF returns a Rule that invokes the receiver and then calls the given CallerFunc
func (r Rule) CallerF(cf calls.CallerFunc) Rule {
	return r.Caller(calls.Caller(cf))
}

// Call implements the Caller interface for Rule
func (r Rule) Call(ctx context.Context, c *scheduler.Call) (mesos.Response, error) {
	if r == nil {
		return nil, nil
	}
	_, _, resp, err := r(ctx, c, nil, nil, chainIdentity)
	return resp, err
}

// Call implements the Caller interface for Rules
func (rs Rules) Call(ctx context.Context, c *scheduler.Call) (mesos.Response, error) {
	return Rule(rs.Eval).Call(ctx, c)
}

var (
	_ = calls.Caller(Rule(nil))
	_ = calls.Caller(Rules(nil))
)

// WithFrameworkID returns a Rule that injects a framework ID to outgoing calls, with the following exceptions:
//   - SUBSCRIBE calls are never modified (schedulers should explicitly construct such calls)
//   - calls are not modified when the detected framework ID is ""
func WithFrameworkID(frameworkID func() string) Rule {
	return func(ctx context.Context, c *scheduler.Call, r mesos.Response, err error, ch Chain) (context.Context, *scheduler.Call, mesos.Response, error) {
		// never overwrite framework ID for subscribe calls; the scheduler must do that part
		if c.GetType() != scheduler.Call_SUBSCRIBE {
			if fid := frameworkID(); fid != "" {
				c2 := *c
				c2.FrameworkID = &mesos.FrameworkID{Value: fid}
				c = &c2
			}
		}
		return ch(ctx, c, r, err)
	}
}
