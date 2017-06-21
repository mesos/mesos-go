package framing

import (
	"testing"
)

func TestError(t *testing.T) {
	a := Error("a")
	if "a" != string(a) {
		t.Errorf("identity/sanity check failed")
	}
	if "a" != a.Error() {
		t.Errorf("expected 'a' instead of %q", a.Error())
	}
}
