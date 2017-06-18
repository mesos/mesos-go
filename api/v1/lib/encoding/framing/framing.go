package framing

type Error string

func (err Error) Error() string { return string(err) }

const (
	ErrorUnderrun       = Error("frame underrun, unexpected EOF")
	ErrorBadSize        = Error("bad frame size")
	ErrorOversizedFrame = Error("oversized frame, max size exceeded")
)

type (
	// Reader generates data frames from some source, returning io.EOF with the final frame.
	Reader interface {
		ReadFrame() (frame []byte, err error)
	}

	// ReaderFunc is the functional adaptation of Reader
	ReaderFunc func() ([]byte, error)
)

func (f ReaderFunc) ReadFrame() ([]byte, error) { return f() }

var _ = Reader(ReaderFunc(nil))
