// +build ignore

package main

import (
	"os"
	"text/template"
)

func main() {
	Run(handlersTemplate, nil, os.Args...)
}

var handlersTemplate = template.Must(template.New("").Parse(`package {{.Package}}

// go generate {{.Args}}
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
{{range .Imports}}
	{{ printf "%q" . -}}
{{end}}
)

{{.RequireType "C" -}}{{/* C is assumed to be a pointer type */ -}}
type (
	// Request generates a Call that's sent to a Mesos agent. Subsequent invocations are expected to
	// yield equivalent calls. Intended for use w/ non-streaming requests to an agent.
	Request interface {
		Call() {{.Type "C"}}
	}

	// RequestFunc is the functional adaptation of Request.
	RequestFunc func() {{.Type "C"}}

	// RequestStreaming generates a Call that's send to a Mesos agent. Subsequent invocations MAY generate
	// different Call objects. No more Call objects are expected once a nil is returned to signal the end of
	// of the request stream.
	RequestStreaming interface {
		Request
		IsStreaming()
	}

	// RequestStreamingFunc is the functional adaptation of RequestStreaming.
	RequestStreamingFunc func() {{.Type "C"}}

	// Send issues a Request to a Mesos agent and properly manages Call-specific mechanics.
	Sender interface {
		Send(context.Context, Request) (mesos.Response, error)
	}

	// SenderFunc is the functional adaptation of the Sender interface
	SenderFunc func(context.Context, Request) (mesos.Response, error)
)

func (f RequestFunc) Call() {{.Type "C"}} { return f() }
func (f RequestFunc) Marshaler() encoding.Marshaler {
	call := f()
	// avoid returning ({{.Type "C"}})(nil) for interface type
	if call != nil {
		return call
	}
	return nil
}

func (f RequestStreamingFunc) Call() {{.Type "C"}} { return f() }
func (f RequestStreamingFunc) Marshaler() encoding.Marshaler {
	call := f()
	// avoid returning ({{.Type "C"}})(nil) for interface type
	if call != nil {
		return call
	}
	return nil
}
func (f RequestStreamingFunc) IsStreaming() {}

var (
	_ = Request(RequestFunc(nil))
	_ = RequestStreaming(RequestStreamingFunc(nil))
	_ = Sender(SenderFunc(nil))
)

// NonStreaming returns a RequestFunc that always generates the same Call.
func NonStreaming(c {{.Type "C"}}) RequestFunc { return func() {{.Type "C"}} { return c } }

// FromChan returns a streaming request that fetches calls from the given channel until it closes.
// If a nil chan is specified then the returned func will always generate nil.
func FromChan(ch <-chan {{.Type "C"}}) RequestStreamingFunc {
	if ch == nil {
		// avoid blocking forever if we're handed a nil chan
		return func() {{.Type "C"}} { return nil }
	}
	return func() {{.Type "C"}} {
		m, ok := <-ch
		if !ok {
			return nil
		}
		return m
	}
}

// Send implements the Sender interface for SenderFunc
func (f SenderFunc) Send(ctx context.Context, r Request) (mesos.Response, error) {
	return f(ctx, r)
}

// SendNoData is a convenience func that executes the given Call using the provided Sender
// and always drops the response data.
func SendNoData(ctx context.Context, sender Sender, r Request) error {
	resp, err := sender.Send(ctx, r)
	if resp != nil {
		resp.Close()
	}
	return err
}
`))
