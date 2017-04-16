package mesos

import (
	"io"

	"github.com/mesos/mesos-go/api/v1/lib/encoding"
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

// ResponseWrapper delegates to optional handler funcs for invocations of Response methods.
type ResponseWrapper struct {
	Response    Response
	CloseFunc   func() error
	DecoderFunc func() encoding.Decoder
}

func (wrapper *ResponseWrapper) Close() error {
	if wrapper.CloseFunc != nil {
		return wrapper.CloseFunc()
	}
	if wrapper.Response != nil {
		return wrapper.Response.Close()
	}
	return nil
}

func (wrapper *ResponseWrapper) Decoder() encoding.Decoder {
	if wrapper.DecoderFunc != nil {
		return wrapper.DecoderFunc()
	}
	return wrapper.Response.Decoder()
}
