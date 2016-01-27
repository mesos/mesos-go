package framing

type Reader interface {
	ReadFrame(buf []byte) (endOfFrame bool, n int, err error)
}
