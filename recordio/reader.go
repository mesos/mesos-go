package recordio

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
)

// NewReader returns an io.Reader that unpacks the data read from r out of
// RecordIO framing before returning it.
func NewReader(r io.Reader) io.Reader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &reader{r: br}
}

type reader struct {
	r       *bufio.Reader
	pending uint64
}

func (rr *reader) Read(p []byte) (n int, err error) {
	for err == nil && len(p) > 0 {
		if rr.pending == 0 {
			if n > 0 && !rr.more() {
				// We've read enough. Don't potentially block reading the next header.
				break
			}
			rr.pending, err = rr.size()
			continue
		}
		read, hi := 0, min(rr.pending, uint64(len(p)))
		read, err = rr.r.Read(p[:hi])
		n += read
		p = p[read:]
		rr.pending -= uint64(read)
	}
	return n, err
}

func (rr *reader) more() bool {
	peek, err := rr.r.Peek(rr.r.Buffered())
	return err != nil && bytes.IndexByte(peek, '\n') >= 0
}

func (rr *reader) size() (uint64, error) {
	header, err := rr.r.ReadSlice('\n')
	if err != nil {
		return 0, err
	}
	// NOTE(tsenart): https://github.com/golang/go/issues/2632
	return strconv.ParseUint(string(bytes.TrimSpace(header)), 10, 64)
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
