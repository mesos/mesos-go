package callrules

// go generate -import github.com/mesos/mesos-go/api/v1/lib -import github.com/mesos/mesos-go/api/v1/lib/scheduler -type E:*scheduler.Call:&scheduler.Call{} -type ET:scheduler.Call_Type -type Z:mesos.Response:&mesos.ResponseWrapper{} -output metrics_generated.go
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func TestMetrics(t *testing.T) {
	var (
		i   int
		ctx = context.Background()
		p   = &scheduler.Call{}
		a   = errors.New("a")
		h   = func(f func() error, _ ...string) error {
			i++
			return f()
		}
		r = Metrics(h, func(_ context.Context, _ *scheduler.Call) []string { return nil })
	)
	var zp = &mesos.ResponseWrapper{}
	for ti, tc := range []struct {
		ctx context.Context
		e   *scheduler.Call
		z   mesos.Response
		err error
	}{
		{ctx, p, zp, a},
		{ctx, p, zp, nil},
		
		{ctx, p, nil, a},
		{ctx, nil, zp, a},
	} {
		c, e, z, err := r.Eval(tc.ctx, tc.e, tc.z, tc.err, ChainIdentity)
		if !reflect.DeepEqual(c, tc.ctx) {
			t.Errorf("test case %d: expected context %q instead of %q", ti, tc.ctx, c)
		}
		if !reflect.DeepEqual(e, tc.e) {
			t.Errorf("test case %d: expected event %q instead of %q", ti, tc.e, e)
		}
		if !reflect.DeepEqual(z, tc.z) {
			t.Errorf("expected return object %q instead of %q", z, tc.z)
		}
		if !reflect.DeepEqual(err, tc.err) {
			t.Errorf("test case %d: expected error %q instead of %q", ti, tc.err, err)
		}
		if y := ti + 1; y != i {
			t.Errorf("test case %d: expected count %q instead of %q", ti, y, i)
		}
	}
}
