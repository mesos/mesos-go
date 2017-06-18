package framing

import (
	"fmt"
	"io"
)

type (
	// UnmarshalFunc translates bytes to objects
	UnmarshalFunc func([]byte, interface{}) error

	// Decoder reads and decodes Protobuf messages from an io.Reader.
	Decoder struct {
		r   Reader
		buf []byte
		uf  UnmarshalFunc
	}
)

// NewDecoder returns a new Decoder that reads from the given io.Reader.
// If r does not also implement StringReader, it will be wrapped in a bufio.Reader.
func NewDecoder(r Reader, uf UnmarshalFunc) *Decoder {
	return &Decoder{r: r, buf: make([]byte, 4096), uf: uf}
}

// MaxSize is the maximum decodable message size.
const MaxSize = 4 << 20 // 4MB

var (
	// ErrSize is returned by Decode calls when a message would exceed the maximum
	// allowed size.
	ErrSize = fmt.Errorf("proto: message exceeds %dMB", MaxSize>>20)
)

// Decode reads the next Protobuf-encoded message from its input and stores it
// in the value pointed to by m. If m isn't a proto.Message, Decode will panic.
func (d *Decoder) Decode(m interface{}) error {
	// Note: the buf returned by ReadFrame will change over time, it can't be sub-sliced
	// and then those sub-slices retained. Examination of generated proto code seems to indicate
	// that byte buffers are copied vs. referenced by sub-slice (gogo protoc).
	frame, err := d.r.ReadFrame()
	if err == nil || err == io.EOF {
		if err2 := d.uf(frame, m); err2 != nil {
			err = err2
		}
	}
	return err
}
