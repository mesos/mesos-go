package calls_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
)

func TestRole(t *testing.T) {
	var (
		rolesNone []string
		roleX     = []string{"x"}
	)
	for ti, tc := range []struct {
		call  *scheduler.Call
		roles []string
	}{
		{calls.Revive(calls.Roles()), rolesNone},
		{calls.Suppress(calls.Roles()), rolesNone},

		{calls.Revive(calls.Roles(roleX...)), roleX},
		{calls.Suppress(calls.Roles(roleX...)), roleX},
	} {
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
