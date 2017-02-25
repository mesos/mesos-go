package proto_test

import (
	"bytes"
	"testing"

	. "github.com/mesos/mesos-go/api/v1/lib/encoding/proto"
)

type FakeMessage struct {
	Hello string `protobuf:"bytes,1,req,name=hello"`
}

func (f *FakeMessage) Reset()         {}
func (f *FakeMessage) ProtoMessage()  {}
func (f *FakeMessage) String() string { return f.Hello }

func TestEncoder(t *testing.T) {
	// write a proto message, validate that we're actually marshaling proto
	buf := bytes.Buffer{}
	enc := NewEncoder(&buf)
	err := enc.Encode(&FakeMessage{"hello"})

	if err != nil {
		t.Fatal(err)
	}

	data := string(buf.Bytes())
	if data != "\n\x05hello" {
		// \n   == 00001 010 == {field-id} {tag-type}
		// \x05 == length of the string
		t.Fatalf("expected `hello` instead of %q", data)
	}

	// write a non-proto message, verify panic
	caughtPanic := false
	func() {
		defer func() {
			if x := recover(); x != nil {
				caughtPanic = true
			}
		}()
		enc.Encode("hello")
		t.Fatal("expected panic, but Encode completed normally")
	}()
	if !caughtPanic {
		t.Fatal("Encode failed to complete normally, but we didn't see a panic? should never happen")
	}
}
