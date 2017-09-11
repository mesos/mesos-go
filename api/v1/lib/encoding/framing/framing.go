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

	// ReaderFunc is the functional adaptation of Reader.
	ReaderFunc func() ([]byte, error)

	// Writer sends whole frames to some endpoint; returns io.ErrShortWrite if the frame is only partially written.
	Writer interface {
		WriteFrame(frame []byte) error
	}

	// WriterFunc is the functional adaptation of Writer.
	WriterFunc func([]byte) error
)

func (f ReaderFunc) ReadFrame() ([]byte, error) { return f() }
func (f WriterFunc) WriteFrame(b []byte) error  { return f(b) }

var _ = Reader(ReaderFunc(nil))
var _ = Writer(WriterFunc(nil))
