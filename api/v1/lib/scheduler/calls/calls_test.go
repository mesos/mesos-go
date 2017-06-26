package calls_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
)

func TestRole(t *testing.T) {
	type outcome int
	const (
		outcomeChanged = outcome(iota)
		outcomePanic
		outcomeUnchanged

		roleX = "x"
		roleY = "y"
	)
	for ti, tc := range []struct {
		call    *scheduler.Call
		role    string
		outcome outcome
	}{
		{nil, "", outcomeUnchanged},
		{nil, roleX, outcomeUnchanged},
		{&scheduler.Call{}, "", outcomePanic},
		{&scheduler.Call{}, roleX, outcomePanic},
		{&scheduler.Call{Type: scheduler.Call_SUBSCRIBE}, "", outcomePanic},
		{&scheduler.Call{Type: scheduler.Call_SUBSCRIBE}, roleX, outcomePanic},

		{&scheduler.Call{Type: scheduler.Call_REVIVE}, "", outcomeUnchanged},
		{&scheduler.Call{Type: scheduler.Call_REVIVE, Revive: &scheduler.Call_Revive{}}, "", outcomeUnchanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS}, "", outcomeUnchanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS, Suppress: &scheduler.Call_Suppress{}}, "", outcomeUnchanged},

		{&scheduler.Call{Type: scheduler.Call_REVIVE}, roleX, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_REVIVE, Revive: &scheduler.Call_Revive{}}, roleX, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS}, roleX, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS, Suppress: &scheduler.Call_Suppress{}}, roleX, outcomeChanged},

		{&scheduler.Call{Type: scheduler.Call_REVIVE, Revive: &scheduler.Call_Revive{Role: proto.String(roleY)}}, roleX, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS, Suppress: &scheduler.Call_Suppress{Role: proto.String(roleY)}}, roleX, outcomeChanged},

		{&scheduler.Call{Type: scheduler.Call_REVIVE, Revive: &scheduler.Call_Revive{Role: proto.String(roleY)}}, "", outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS, Suppress: &scheduler.Call_Suppress{Role: proto.String(roleY)}}, "", outcomeChanged},
	} {
		var (
			caught interface{}
			before = proto.Clone(tc.call).(*scheduler.Call)
		)
		func() {
			defer func() {
				caught = recover()
			}()
			_ = tc.call.With(calls.Role(tc.role))
		}()
		switch tc.outcome {
		case outcomePanic:
			if caught == nil {
				t.Errorf("test case %d failed: expected panic", ti)
			}
		case outcomeUnchanged:
			if !reflect.DeepEqual(before, tc.call) {
				t.Errorf("test case %d failed: expected unchanged call instead of: %#v ", ti, tc.call)
			}
		case outcomeChanged:
			role, hasRole := func() (string, bool) {
				switch tc.call.Type {
				case scheduler.Call_SUPPRESS:
					return tc.call.Suppress.GetRole(), tc.call.Suppress.Role != nil
				case scheduler.Call_REVIVE:
					return tc.call.Revive.GetRole(), tc.call.Revive.Role != nil
				default:
					panic(fmt.Sprintf("test case %d failed: unsupported call type: %v", ti, tc.call.Type))
				}
			}()
			if hasRole != (tc.role != "") {
				if hasRole {
					t.Errorf("test case %d failed: expected no role instead of %q", ti, role)
				} else {
					t.Errorf("test case %d failed: expected role %q instead of no role", ti, tc.role)
				}
			}
			if hasRole && tc.role != role {
				t.Errorf("test case %d failed: expected role %q instead of %q", ti, tc.role, role)
			}
		}
	}
}
