package scheduler

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/encoding"
)

// DefaultOpts holds the default Scheduler configuration.
var DefaultOpts = []Opt{
	URL("https://127.0.0.1:5050/api/v1/scheduler"),
	Client(&http.Client{Timeout: 5 * time.Second}),
}

// Scheduler is the Mesos HTTP scheduler API client.
type Scheduler struct {
	url   *url.URL
	cli   *http.Client
	hdr   http.Header
	codec encoding.Codec
}

// Opt defines a functional option for the Scheduler type.
type Opt func(*Scheduler) error

// New returns a new Scheduler with the given options applied.
func New(opts ...Opt) (*Scheduler, error) {
	var s Scheduler
	return &s, s.Apply(opts...)
}

// Apply applies the given Opts to a Scheduler and returns an aggregate of
// all non-nill errors returned.
func (s *Scheduler) Apply(opts ...Opt) (err error) {
	errs := make([]string, 0, len(opts))
	for _, opt := range opts {
		if err = opt(s); err != nil {
			errs = append(errs, err.Error())
		}
	}
	return errors.New(strings.Join(errs, "; "))
}

// URL returns an Opt that sets a Scheduler's URL.
func URL(rawurl string) Opt {
	return func(s *Scheduler) error {
		u, err := url.Parse(rawurl)
		if err != nil {
			return err
		}
		s.url = u
		return nil
	}
}

// Client returns an Opt that sets a Scheduler's http.Client.
func Client(c *http.Client) Opt {
	return func(s *Scheduler) error {
		s.cli = c
		return nil
	}
}

// Codec returns an Opt that sets a Scheduler' Codec.
func Codec(c encoding.Codec) Opt {
	return func(s *Scheduler) (err error) {
		s.codec = c
		return nil
	}
}

// Do sends a Call and returns a streaming encoding.Decoder where Events will be
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

	if res.StatusCode != http.StatusAccepted {
		err = fmt.Errorf("scheduler: bad status code: %d", res.StatusCode)
	}

	return s.codec.NewDecoder(res.Body), res.Body, err
}

// A CallOpt is a functional option type for Calls.
type CallOpt func(*Call)

// Apply applies the given CallOpts to the receiving Call, returning it.
func (c *Call) Apply(opts ...CallOpt) *Call {
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
