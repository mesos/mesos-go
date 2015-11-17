package mesos_test

import (
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go"
)

func scalar(f float64) *mesos.Value_Scalar {
	return &mesos.Value_Scalar{
		Value: f,
	}
}

func r(b, e uint64) mesos.Value_Range {
	return mesos.Value_Range{
		Begin: b,
		End:   e,
	}
}

func ranges(x ...mesos.Value_Range) *mesos.Value_Ranges {
	if x == nil {
		return nil
	}
	return &mesos.Value_Ranges{Range: x}
}

func TestValue_Ranges_Add(t *testing.T) {
	for i, tc := range []struct {
		left, right, want *mesos.Value_Ranges
	}{
		{nil, nil, ranges()},
		{nil, ranges(r(0, 0)), ranges(r(0, 0))},
		{ranges(r(0, 0)), nil, ranges(r(0, 0))},
		{ranges(r(0, 0)), ranges(r(0, 0)), ranges(r(0, 0))},
		{ranges(r(0, 1)), ranges(r(0, 0)), ranges(r(0, 1))},
		{ranges(r(0, 0)), ranges(r(0, 1)), ranges(r(0, 1))},
		{ranges(r(0, 1)), ranges(r(1, 1)), ranges(r(0, 1))},
		{ranges(r(0, 1)), ranges(r(1, 2)), ranges(r(0, 2))},
		{ranges(r(3, 4), r(0, 1)), ranges(r(1, 2)), ranges(r(0, 4))},
		{ranges(r(2, 6), r(3, 4), r(0, 1)), ranges(r(1, 2)), ranges(r(0, 6))},
	} {
		preleft := proto.Clone(tc.left).(*mesos.Value_Ranges)
		preright := proto.Clone(tc.right).(*mesos.Value_Ranges)
		x := tc.left.Add(tc.right)
		if !reflect.DeepEqual(x, tc.want) {
			t.Errorf("test case %d failed: expected %#+v instead of %#+v", i, tc.want, x)
		}
		if !preleft.Equal(tc.left) {
			t.Errorf("test case %d failed: before(left) != after(left): %#+v != %#+v", i, preleft, tc.left)
		}
		if !preright.Equal(tc.right) {
			t.Errorf("test case %d failed: before(right) != after(right): %#+v != %#+v", i, preright, tc.right)
		}
	}
}

func TestValue_Ranges_Subtract(t *testing.T) {
	for i, tc := range []struct {
		left, right, want *mesos.Value_Ranges
	}{
		{nil, nil, ranges()},
		{nil, ranges(r(0, 0)), ranges()},
		{ranges(r(0, 0)), nil, ranges(r(0, 0))},
		{ranges(r(0, 0)), ranges(r(0, 0)), ranges()},
		{ranges(r(0, 1)), ranges(r(0, 0)), ranges(r(1, 1))},
		{ranges(r(0, 0)), ranges(r(0, 1)), ranges()},
		{ranges(r(0, 1)), ranges(r(1, 1)), ranges(r(0, 0))},
		{ranges(r(0, 1)), ranges(r(1, 2)), ranges(r(0, 0))},
		{ranges(r(3, 4), r(0, 1)), ranges(r(1, 2)), ranges(r(0, 0), r(3, 4))},
		{ranges(r(2, 6), r(3, 4), r(0, 1)), ranges(r(2, 4)), ranges(r(0, 1), r(5, 6))},
	} {
		preleft := proto.Clone(tc.left).(*mesos.Value_Ranges)
		preright := proto.Clone(tc.right).(*mesos.Value_Ranges)
		x := tc.left.Subtract(tc.right)
		if !reflect.DeepEqual(x, tc.want) {
			t.Errorf("test case %d failed: expected %#+v instead of %#+v", i, tc.want, x)
		}
		if !preleft.Equal(tc.left) {
			t.Errorf("test case %d failed: before(left) != after(left): %#+v != %#+v", i, preleft, tc.left)
		}
		if !preright.Equal(tc.right) {
			t.Errorf("test case %d failed: before(right) != after(right): %#+v != %#+v", i, preright, tc.right)
		}
	}
}

func TestValue_Scalar_Add(t *testing.T) {
	for i, tc := range []struct {
		left, right, want *mesos.Value_Scalar
	}{
		{nil, nil, scalar(0)},
		{nil, scalar(0), scalar(0)},
		{scalar(0), nil, scalar(0)},
		{scalar(0), scalar(0), scalar(0)},
		{scalar(1), scalar(0), scalar(1)},
		{scalar(0), scalar(1), scalar(1)},
		{scalar(-1), scalar(0), scalar(-1)},
		{scalar(1), scalar(-1), scalar(0)},
		{scalar(1), scalar(1), scalar(2)},
		{scalar(-1), scalar(-1), scalar(-2)},
	} {
		preleft := proto.Clone(tc.left).(*mesos.Value_Scalar)
		preright := proto.Clone(tc.right).(*mesos.Value_Scalar)
		x := tc.left.Add(tc.right)
		if !x.Equal(tc.want) {
			t.Errorf("expected %v instead of %v", tc.want, x)
		}
		if !preleft.Equal(tc.left) {
			t.Errorf("test case %d failed: before(left) != after(left): %#+v != %#+v", i, preleft, tc.left)
		}
		if !preright.Equal(tc.right) {
			t.Errorf("test case %d failed: before(right) != after(right): %#+v != %#+v", i, preright, tc.right)
		}
	}
}

func TestValue_Scalar_Subtract(t *testing.T) {
	for i, tc := range []struct {
		left, right, want *mesos.Value_Scalar
	}{
		{nil, nil, scalar(0)},
		{nil, scalar(0), scalar(0)},
		{scalar(0), nil, scalar(0)},
		{scalar(0), scalar(0), scalar(0)},
		{scalar(1), scalar(0), scalar(1)},
		{scalar(0), scalar(1), scalar(-1)},
		{scalar(-1), scalar(0), scalar(-1)},
		{scalar(1), scalar(-1), scalar(2)},
		{scalar(1), scalar(1), scalar(0)},
		{scalar(-1), scalar(-1), scalar(0)},
	} {
		preleft := proto.Clone(tc.left).(*mesos.Value_Scalar)
		preright := proto.Clone(tc.right).(*mesos.Value_Scalar)
		x := tc.left.Subtract(tc.right)
		if !x.Equal(tc.want) {
			t.Errorf("expected %v instead of %v", tc.want, x)
		}
		if !preleft.Equal(tc.left) {
			t.Errorf("test case %d failed: before(left) != after(left): %#+v != %#+v", i, preleft, tc.left)
		}
		if !preright.Equal(tc.right) {
			t.Errorf("test case %d failed: before(right) != after(right): %#+v != %#+v", i, preright, tc.right)
		}
	}
}

func TestValue_Scalar_Compare(t *testing.T) {
	for i, tc := range []struct {
		left, right *mesos.Value_Scalar
		want        int
	}{
		{nil, nil, 0},
		{nil, scalar(0), 0},
		{scalar(0), nil, 0},
		{scalar(0), scalar(0), 0},
		{scalar(1), scalar(0), 1},
		{scalar(0), scalar(1), -1},
		{scalar(-1), scalar(0), -1},
		{scalar(1), scalar(-1), 1},
		{scalar(1), scalar(1), 0},
		{scalar(-1), scalar(-1), 0},
	} {
		preleft := proto.Clone(tc.left).(*mesos.Value_Scalar)
		preright := proto.Clone(tc.right).(*mesos.Value_Scalar)
		x := tc.left.Compare(tc.right)
		if x != tc.want {
			t.Errorf("test case %d failed: expected %v instead of %v", i, tc.want, x)
		}
		if !preleft.Equal(tc.left) {
			t.Errorf("test case %d failed: before(left) != after(left): %#+v != %#+v", i, preleft, tc.left)
		}
		if !preright.Equal(tc.right) {
			t.Errorf("test case %d failed: before(right) != after(right): %#+v != %#+v", i, preright, tc.right)
		}
	}
}
