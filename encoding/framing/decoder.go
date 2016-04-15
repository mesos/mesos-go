package framing

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
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
	var (
		buf     = d.buf
		readlen = 0
	)
	for {
		eof, nr, err := d.r.ReadFrame(buf)
		if err != nil {
			return err
		}

		readlen += nr
		if readlen > MaxSize {
			return ErrSize
		}

		if eof {
			return d.uf(d.buf[:readlen], m.(proto.Message))
		}

		if len(buf) == nr {
			// readlen and len(d.buf) are the same here
			newbuf := make([]byte, readlen+4096)
			copy(newbuf, d.buf)
			d.buf = newbuf
			buf = d.buf[readlen:]
		} else {
			buf = buf[nr:]
		}
	}
}
