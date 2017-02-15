package proto

import (
	"errors"
	"testing"

)

//Mock the Proto Message to increase coverage
type MockProtoMessage struct {
	IsReturnError bool
	content []byte
}
//Reset implements proto.Message interface
func (mpm MockProtoMessage) Reset() {

}
//Reset implements proto.Marshaller interface
func (mpm MockProtoMessage) Marshal () ([]byte, error) {
 if mpm.IsReturnError {
	 return nil, errors.New("Mock Marshall error")
 } else {
	 return mpm.content, nil
 }
}

//Reset implements proto.Message interface
func (mpm MockProtoMessage) String() string {
	return string(mpm.content)
}

//Reset implements proto.Message interface
func (mpm MockProtoMessage) ProtoMessage() {

}

//A Mock writer for this object Encoder
type MockWriter struct {
	content []byte
}

//Write implements Writer interface
func (mw *MockWriter) Write(p []byte) (n int, err error) {

	if p == nil {
		return -1, errors.New("MockWriter trying to write nil object")
	}

	mw.content = p
	return len(p), nil
}



func TestEncode_NilMessage(t *testing.T) {

	e := NewEncoder(&MockWriter{})

	m:= MockProtoMessage{IsReturnError:true}

	err := e.Encode(&m)

	if err == nil {
		t.Fatalf("Should Produce error = %v", err)
	}
}


func TestEncode_ValidMessage(t *testing.T) {

	mw := &MockWriter{}

	e := NewEncoder(mw)

	m:= MockProtoMessage{IsReturnError:false, content:[]byte{'m','e','s','o','s'}}

	err := e.Encode(&m)

	if err != nil {
		t.Fatalf("Should not produce Error=%v", err)
	}
	if len(mw.content) != len(m.content) {
		t.Fatal("The content should be exaclty same")
	}
}