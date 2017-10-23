package framing

import (
	"bytes"
	"io"
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

func TestReadAll(t *testing.T) {
	r := ReadAll(bytes.NewBufferString(""))
	buf, err := r.ReadFrame()
	if len(buf) != 0 {
		t.Errorf("expected zero length frame instead of %+v", buf)
	}
	if err != io.EOF {
		t.Errorf("expected EOF instead of %+v", err)
	}

	r = ReadAll(bytes.NewBufferString("foo"))
	buf, err = r.ReadFrame()
	if err != nil {
		t.Fatalf("unexpected error %+v", err)
	}
	if string(buf) != "foo" {
		t.Errorf("expected 'foo' instead of %q", string(buf))
	}

	// read again, now that there's no more data
	buf, err = r.ReadFrame()
	if len(buf) != 0 {
		t.Errorf("expected zero length frame instead of %+v", buf)
	}
	if err != io.EOF {
		t.Errorf("expected EOF instead of %+v", err)
	}
}
