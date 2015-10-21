package mesos

import (
	"io"

	"github.com/mesos/mesos-go/encoding"
)

// A Client represents a Mesos HTTP API client which can send Calls and return
// a streaming Decoder from which callers can read Events from, an io.Closer to
// close the event stream on graceful termination and an error in case of failure.
type Client interface {
	Do(encoding.Marshaler) (encoding.Decoder, io.Closer, error)
}
