package proto

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/gogo/protobuf/proto"
)

// NewDecoder returns a new Decoder that reads from the given io.Reader.
// If r does not also implement io.ByteReader, it will be wrapped in a bufio.Reader.
func NewDecoder(r io.Reader) *Decoder {
	d := Decoder{r: r, buf: make([]byte, 4096)}
	if _, ok := d.r.(io.ByteReader); !ok {
		d.r = bufio.NewReader(d.r)
	}
	return &d
}

// A Decoder reads and decodes Protobuf messages from an io.Reader.
type Decoder struct {
	r   io.Reader
	buf []byte
}

// MaxSize is the maximum decodable message size.
const MaxSize = 4 << 20 // 4MB

var (
	// ErrSize is returned by Decode calls when a message would exceed the maximum
	// allowed size.
	ErrSize = fmt.Errorf("proto: message exceeds %fMB", MaxSize>>20)
)

// Decode reads the next Protobuf-encoded message from its input and stores it
// in the value pointed to by m. If m isn't a proto.Message, Decode will panic.
func (d *Decoder) Decode(m interface{}) error {
	if n, err := binary.ReadUvarint(d.r.(io.ByteReader)); err != nil {
		return err
	} else if n > MaxSize {
		return ErrSize
	} else if uint64(len(d.buf)) < n {
		d.buf = make([]byte, n)
	} else if nr, err := io.ReadFull(d.r, d.buf[:n]); err != nil {
		return err
	} else {
		return proto.Unmarshal(d.buf[:nr], m.(proto.Message))
	}
	panic("unreachable")
}

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
