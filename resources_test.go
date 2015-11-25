package mesos_test

import (
	"testing"

	"github.com/mesos/mesos-go"
)

func TestResource_IsEmpty(t *testing.T) {
	for i, tc := range []struct {
		r     *mesos.Resource
		wants bool
	}{
		{nil, true},
		{new(mesos.Resource), true},
		{resource(valueScalar(0)), true},
		{resource(valueSet()), true},
		{resource(valueSet([]string{}...)), true},
		{resource(valueSet()), true},
		{resource(valueSet("")), false},
		{resource(valueRange()), true},
		{resource(valueRange(pair(0, 0))), false},
	} {
		actual := tc.r.IsEmpty()
		if tc.wants != actual {
			t.Errorf("test case %d failed: wants (%t) != actual (%t)", i, tc.wants, actual)
		}
	}
}

func TestResources_PlusAll(t *testing.T) {
	for i, tc := range []struct {
		r1, r2      mesos.Resources
		wants       mesos.Resources
		wantsCPU    float64
		wantsMemory uint64
	}{
		{nil, nil, nil, 0, 0},
		{resources(), resources(), resources(), 0, 0},
		// simple scalars, same roles for everything
		{resources(
			resource(name("cpus"), valueScalar(1), role("*")),
			resource(name("mem"), valueScalar(5), role("*")),
		), resources(
			resource(name("cpus"), valueScalar(2), role("*")),
			resource(name("mem"), valueScalar(10), role("*")),
		), resources(
			resource(name("cpus"), valueScalar(3), role("*")),
			resource(name("mem"), valueScalar(15), role("*")),
		), 3, 15},
		// simple scalars, differing roles
		{resources(
			resource(name("cpus"), valueScalar(1), role("role1")),
			resource(name("cpus"), valueScalar(3), role("role2")),
		), resources(
			resource(name("cpus"), valueScalar(5), role("role1")),
		), resources(
			resource(name("cpus"), valueScalar(6), role("role1")),
			resource(name("cpus"), valueScalar(3), role("role2")),
		), 9, 0},
	} {
		backup := tc.r1.Clone()

		// PlusAll preserves the left operand
		actual := tc.r1.PlusAll(tc.r2)
		if !tc.wants.Equivalent(actual) {
			t.Errorf("test case %d failed: wants (%v) != actual (%v)", i, tc.wants, actual)
		}
		if !backup.Equivalent(tc.r1) {
			t.Errorf("test case %d failed: backup (%v) != r1 (%v)", i, backup, tc.r1)
		}

		// AddAll mutates the left operand
		tc.r1.AddAll(tc.r2)
		if !tc.wants.Equivalent(tc.r1) {
			t.Errorf("test case %d failed: wants (%v) != r1 (%v)", i, tc.wants, tc.r1)
		}

		cpus, ok := tc.r1.CPUs()
		if !ok && tc.wantsCPU > 0 {
			t.Errorf("test case %d failed: failed to obtain total CPU resources", i)
		} else if cpus != tc.wantsCPU {
			t.Errorf("test case %d failed: wants cpu (%v) != r1 cpu (%v)", i, tc.wantsCPU, cpus)
		}

		mem, ok := tc.r1.Memory()
		if !ok && tc.wantsMemory > 0 {
			t.Errorf("test case %d failed: failed to obtain total memory resources", i)
		} else if mem != tc.wantsMemory {
			t.Errorf("test case %d failed: wants mem (%v) != r1 mem (%v)", i, tc.wantsMemory, mem)
		}
	}
}

// functional resource modifier
type resourceOpt func(*mesos.Resource)

func resource(opt ...resourceOpt) *mesos.Resource {
	if len(opt) == 0 {
		return nil
	}
	r := &mesos.Resource{}
	for _, f := range opt {
		f(r)
	}
	return r
}

func name(x string) resourceOpt { return func(r *mesos.Resource) { r.Name = x } }
func role(x string) resourceOpt { return func(r *mesos.Resource) { r.Role = &x } }

func valueScalar(x float64) resourceOpt {
	return func(r *mesos.Resource) {
		r.Type = mesos.SCALAR.Enum()
		r.Scalar = &mesos.Value_Scalar{Value: x}
	}
}

func valueSet(x ...string) resourceOpt {
	return func(r *mesos.Resource) {
		r.Type = mesos.SET.Enum()
		r.Set = &mesos.Value_Set{Item: x}
	}
}

type rangeOpt func(*mesos.Ranges)

func pair(bp, ep uint64) rangeOpt {
	return func(rs *mesos.Ranges) {
		*rs = append(*rs, mesos.Value_Range{Begin: bp, End: ep}).Squash()
	}
}

func valueRange(p ...rangeOpt) resourceOpt {
	return func(r *mesos.Resource) {
		rs := mesos.Ranges(nil)
		for _, f := range p {
			f(&rs)
		}
		r.Type = mesos.RANGES.Enum()
		r.Ranges = &mesos.Value_Ranges{Range: rs}
	}
}

func resources(r ...*mesos.Resource) mesos.Resources {
	return mesos.Resources(r)
}
