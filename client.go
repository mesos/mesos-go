package mesos

import (
	"io"

	"github.com/mesos/mesos-go/encoding"
)

// A Client represents a Mesos API client which can send Calls and return
// a streaming Decoder from which callers can read Events from, an io.Closer to
// close the event stream on graceful termination and an error in case of failure.
type Client interface {
	Do(encoding.Marshaler) (Response, error)
}

// ClientFunc is a functional variant (and implementation) of the Client interface
type ClientFunc func(encoding.Marshaler) (Response, error)

// Do implements Client
func (cf ClientFunc) Do(m encoding.Marshaler) (Response, error) { return cf(m) }

// Response captures the output of a Mesos API operation. Callers are responsible for invoking
// Close when they're finished processing the response otherwise there may be connection leaks.
type Response interface {
	io.Closer
	Decoder() encoding.Decoder
}
