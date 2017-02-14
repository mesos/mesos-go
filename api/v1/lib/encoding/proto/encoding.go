package proto

import (
	"io"

	"github.com/gogo/protobuf/proto"
)

// NewEncoder returns a new Encoder that writes to the given io.Writer.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// An Encoder encodes and writes Protobuf messages to an io.Writer.
type Encoder struct{ w io.Writer }

// Encode writes the given Protobuf-encoded message m to its io.Writer. If m
// isn't a proto.Message, Encode will panic.
func (e *Encoder) Encode(m interface{}) error {
	bs, err := proto.Marshal(m.(proto.Message))
	if err != nil {
		return err
	}
	_, err = e.w.Write(bs)
	return err
}
