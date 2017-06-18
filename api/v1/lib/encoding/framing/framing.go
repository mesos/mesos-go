package framing

type Error string

func (err Error) Error() string { return string(err) }

const (
	ErrorUnderrun       = Error("frame underrun, unexpected EOF")
	ErrorBadSize        = Error("bad frame size")
	ErrorOversizedFrame = Error("oversized frame, max size exceeded")
)

type Reader interface {
	ReadFrame() (frame []byte, err error)
}
