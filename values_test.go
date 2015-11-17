package mesos_test

import (
	"testing"

	"github.com/mesos/mesos-go"
)

func scalar(f float64) *mesos.Value_Scalar {
	return &mesos.Value_Scalar{
		Value: f,
	}
}

func TestValue_Scalar_Add(t *testing.T) {
	for _, tc := range []struct {
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
		x := tc.left.Add(tc.right)
		if !x.Equal(tc.want) {
			t.Errorf("expected %v instead of %v", tc.want, x)
		}
	}
}

func TestValue_Scalar_Subtract(t *testing.T) {
	for _, tc := range []struct {
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
		x := tc.left.Subtract(tc.right)
		if !x.Equal(tc.want) {
			t.Errorf("expected %v instead of %v", tc.want, x)
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
		x := tc.left.Compare(tc.right)
		if x != tc.want {
			t.Errorf("test case %d failed: expected %v instead of %v", i, tc.want, x)
		}
	}
}
