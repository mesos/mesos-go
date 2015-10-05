package scheduler

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mesos/mesos-go/proto"
)

// DefaultOpts holds the default Scheduler configuration.
var DefaultOpts = []Opt{
	URL("https://127.0.0.1:5050/api/v1/scheduler"),
	Client(&http.Client{Timeout: 5 * time.Second}),
}

// Scheduler is the Mesos HTTP scheduler API client.
type Scheduler struct {
	url *url.URL
	cli *http.Client
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

func (s *Scheduler) Do(c *Call) (*proto.Decoder, error) {
	payload, err := c.Marshal()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", s.url.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")

	res, err := s.cli.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusAccepted {
		err = fmt.Errorf("scheduler: bad status code: %d", res.StatusCode)
	}

	return proto.NewDecoder(res.Body), err
}
