package latch

import "sync"

type Interface interface {
	Done() <-chan struct{}
	Close()
	Closed() bool
}

// Latch is Closed by default and should be Reset() in order to be useful.
type L struct {
	sync.Once
	line chan struct{}
}

func New() Interface {
	return new(L).Reset()
}

func (l *L) Done() <-chan struct{} { return l.line }

func (l *L) Close() {
	l.Do(func() {
		defer func() { _ = recover() }() // swallow any panics here
		close(l.line)
	})
}

func (l *L) Closed() (result bool) {
	select {
	case <-l.line:
		result = true
	default:
	}
	return
}

// Reset clears the state of the latch, not safe to execute concurrently with other L methods.
func (l *L) Reset() *L {
	l.line, l.Once = make(chan struct{}), sync.Once{}
	return l
}
