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
	)
	var (
		rolesNone []string
		roleX     = []string{"x"}
		roleY     = []string{"y"}
	)
	for ti, tc := range []struct {
		call    *scheduler.Call
		roles   []string
		outcome outcome
	}{
		{nil, rolesNone, outcomeUnchanged},
		{nil, roleX, outcomeUnchanged},
		{&scheduler.Call{}, rolesNone, outcomePanic},
		{&scheduler.Call{}, roleX, outcomePanic},
		{&scheduler.Call{Type: scheduler.Call_SUBSCRIBE}, rolesNone, outcomePanic},
		{&scheduler.Call{Type: scheduler.Call_SUBSCRIBE}, roleX, outcomePanic},

		{&scheduler.Call{Type: scheduler.Call_REVIVE}, rolesNone, outcomeUnchanged},
		{&scheduler.Call{Type: scheduler.Call_REVIVE, Revive: &scheduler.Call_Revive{}}, rolesNone, outcomeUnchanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS}, rolesNone, outcomeUnchanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS, Suppress: &scheduler.Call_Suppress{}}, rolesNone, outcomeUnchanged},

		{&scheduler.Call{Type: scheduler.Call_REVIVE}, roleX, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_REVIVE, Revive: &scheduler.Call_Revive{}}, roleX, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS}, roleX, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS, Suppress: &scheduler.Call_Suppress{}}, roleX, outcomeChanged},

		{&scheduler.Call{Type: scheduler.Call_REVIVE, Revive: &scheduler.Call_Revive{Roles: roleY}}, roleX, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS, Suppress: &scheduler.Call_Suppress{Roles: roleY}}, roleX, outcomeChanged},

		{&scheduler.Call{Type: scheduler.Call_REVIVE, Revive: &scheduler.Call_Revive{Roles: roleY}}, rolesNone, outcomeChanged},
		{&scheduler.Call{Type: scheduler.Call_SUPPRESS, Suppress: &scheduler.Call_Suppress{Roles: roleY}}, rolesNone, outcomeChanged},
	} {
		var (
			caught interface{}
			before = proto.Clone(tc.call).(*scheduler.Call)
		)
		func() {
			defer func() {
				caught = recover()
			}()
			_ = tc.call.With(calls.Roles(tc.roles...))
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
			roles, hasRole := func() ([]string, bool) {
				switch tc.call.Type {
				case scheduler.Call_SUPPRESS:
					return tc.call.Suppress.GetRoles(), len(tc.call.Suppress.Roles) > 0
				case scheduler.Call_REVIVE:
					return tc.call.Revive.GetRoles(), len(tc.call.Revive.Roles) > 0
				default:
					panic(fmt.Sprintf("test case %d failed: unsupported call type: %v", ti, tc.call.Type))
				}
			}()
			if hasRole != (len(tc.roles) > 0) {
				if hasRole {
					t.Errorf("test case %d failed: expected no role instead of %q", ti, roles)
				} else {
					t.Errorf("test case %d failed: expected role %q instead of no role", ti, tc.roles)
				}
			}
			if hasRole && !reflect.DeepEqual(tc.roles, roles) {
				t.Errorf("test case %d failed: expected role %q instead of %q", ti, tc.roles, roles)
			}
		}
	}
}
