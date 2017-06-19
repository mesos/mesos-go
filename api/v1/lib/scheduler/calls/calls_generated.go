package calls

// go generate -import github.com/mesos/mesos-go/api/v1/lib/scheduler -type C:*scheduler.Call
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"

	"github.com/mesos/mesos-go/api/v1/lib"

	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

type (
	// Caller is the public interface this framework scheduler's should consume
	Caller interface {
		// Call issues a call to Mesos and properly manages call-specific HTTP response headers & data.
		Call(context.Context, *scheduler.Call) (mesos.Response, error)
	}

	// CallerFunc is the functional adaptation of the Caller interface
	CallerFunc func(context.Context, *scheduler.Call) (mesos.Response, error)
)

// Call implements the Caller interface for CallerFunc
func (f CallerFunc) Call(ctx context.Context, c *scheduler.Call) (mesos.Response, error) {
	return f(ctx, c)
}

// CallNoData is a convenience func that executes the given Call using the provided Caller
// and always drops the response data.
func CallNoData(ctx context.Context, caller Caller, call *scheduler.Call) error {
	resp, err := caller.Call(ctx, call)
	if resp != nil {
		resp.Close()
	}
	return err
}
