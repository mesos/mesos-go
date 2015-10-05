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

// A Decoder reads and decodes Protobuf messages from an input stream.
type Decoder struct {
	r   io.Reader
	buf []byte
}

// MaxSize is the maximum decodable message size.
const MaxSize = 4 << 20 // 4MB

// ErrSize is returned by Decode calls when a message would exceed the maximum
// allowed size.
var ErrSize = fmt.Errorf("proto: message exceeds %fMB", MaxSize>>20)

// Decode reads the next Protobuf-encoded message from its input and stores it
// in the value pointed to by m.
func (d *Decoder) Decode(m proto.Message) error {
	if n, err := binary.ReadUvarint(d.r.(io.ByteReader)); err != nil {
		return err
	} else if n > MaxSize {
		return ErrSize
	} else if uint64(len(d.buf)) < n {
		d.buf = make([]byte, n)
	} else if nr, err := io.ReadFull(d.r, d.buf[:n]); err != nil {
		return err
	} else {
		return proto.Unmarshal(d.buf[:nr], m)
	}
	panic("unreachable")
}
