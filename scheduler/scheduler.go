package scheduler

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/encoding"
)

// Scheduler is the Mesos HTTP scheduler API client.
type Scheduler struct {
	url   *url.URL
	cli   *http.Client
	hdr   http.Header
	codec encoding.Codec
}

// Opt defines a functional option for the Scheduler type.
type Opt func(*Scheduler)

// With applies the given Opts to a Scheduler and returns an aggregate of
// all non-nill errors returned and itself.
func (s *Scheduler) With(opts ...Opt) *Scheduler {
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// URL returns an Opt that sets a Scheduler's URL.
func URL(u *url.URL) Opt {
	return func(s *Scheduler) { s.url = u }
}

// Client returns an Opt that sets a Scheduler's http.Client.
func Client(c *http.Client) Opt {
	return func(s *Scheduler) { s.cli = c }
}

// Codec returns an Opt that sets a Scheduler' Codec.
func Codec(c encoding.Codec) Opt {
	return func(s *Scheduler) { s.codec = c }
}

var (
	// ErrNotLeader is returned by Do calls that are sent to a non leading Mesos master.
	ErrNotLeader = errors.New("scheduler: call sent to a non-leading master")
	// ErrAuth is returned by Do calls that are not successfully authenticated.
	ErrAuth = errors.New("scheduler: call not authenticated")
	// ErrUnsubscribed is returned by Do calls that are sent before a subscription is established.
	ErrUnsubscribed = errors.New("scheduler: no subscription established")
	// ErrVersion is returned by Do calls that are sent to an	incompatible API version.
	ErrVersion = errors.New("scheduler: incompatible API version")
	// ErrMalformed is returned by Do calls that are malformed.
	ErrMalformed = errors.New("scheduler: malformed request")
	// ErrMediaType is returned by Do calls that are sent with an unsupported media type.
	ErrMediaType = errors.New("scheduler: unsupported media type")
	// ErrRateLimit is returned by Do calls that are rate limited.
	ErrRateLimit = errors.New("scheduler: rate limited")

	// codeErrors maps HTTP response codes to their respective errors.
	codeErrors = map[int]error{
		http.StatusOK:                nil,
		http.StatusAccepted:          nil,
		http.StatusTemporaryRedirect: ErrNotLeader,
		http.StatusBadRequest:        ErrMalformed,
		http.StatusConflict:          ErrVersion,
		http.StatusForbidden:         ErrUnsubscribed,
		http.StatusUnauthorized:      ErrAuth,
		http.StatusNotAcceptable:     ErrMediaType,
		429: ErrRateLimit,
	}
)

// Call calls the scheduler and returns a streaming encoding.Decoder where Events will be
// read from, an io.Closer to close the input stream and an error in case of failure.
func (s *Scheduler) Do(c *Call) (encoding.Decoder, io.Closer, error) {
	var body bytes.Buffer
	if err := s.codec.NewEncoder(&body).Encode(c); err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequest("POST", s.url.String(), &body)
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("Content-Type", s.codec.MediaTypes[0])
	req.Header.Set("Accept", s.codec.MediaTypes[1])

	res, err := s.cli.Do(req)
	if err != nil {
		return nil, nil, err
	}

	return s.codec.NewDecoder(res.Body), res.Body, codeErrors[res.StatusCode]
}

// A CallOpt is a functional option type for Calls.
type CallOpt func(*Call)

// With applies the given CallOpts to the receiving Call, returning it.
func (c *Call) With(opts ...CallOpt) *Call {
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Subscribe returns a CallOpt that makes a Call a Subscribe call.
func Subscribe(force bool, info mesos.FrameworkInfo) CallOpt {
	return func(c *Call) {
		t := Call_SUBSCRIBE
		c.Type = &t
		c.Subscribe = &Call_Subscribe{FrameworkInfo: &info, Force: &force}
	}
}
