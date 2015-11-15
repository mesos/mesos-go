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
