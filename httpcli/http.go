package httpcli

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/mesos/mesos-go/encoding"
)

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

// DoFunc sends an HTTP request and returns an HTTP response.
//
// An error is returned if caused by client policy (such as
// http.Client.CheckRedirect), or if there was an HTTP protocol error. A
// non-2xx response doesn't cause an error.
//
// When err is nil, resp always contains a non-nil resp.Body.
//
// Callers should close resp.Body when done reading from it. If resp.Body is
// not closed, an underlying RoundTripper (typically Transport) may not be able
// to re-use a persistent TCP connection to the server for a subsequent
// "keep-alive" request.
//
// The request Body, if non-nil, will be closed by an underlying Transport,
// even on errors.
type DoFunc func(*http.Request) (*http.Response, error)

// A Client is a Mesos HTTP APIs client.
type Client struct {
	url   string
	do    DoFunc
	hdr   http.Header
	codec *encoding.Codec
}

// New returns a new Client with the given Opts applied.
// Callers are expected to configure the URL, Doer, and Codec options prior to
// invoking Do.
func New(opts ...Opt) *Client { return new(Client).With(opts...) }

// Opt defines a functional option for the HTTP client type.
type Opt func(*Client)

// With applies the given Opts to a Client and returns itself.
func (c *Client) With(opts ...Opt) *Client {
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Do sends a Call and returns a streaming Decoder from which callers can read
// Events from, an io.Closer to close the event stream on graceful termination
// and an error in case of failure. Callers are expected to *always* close a
// non-nil io.Closer if one is returned.
func (c *Client) Do(m encoding.Marshaler) (encoding.Decoder, io.Closer, error) {
	var body bytes.Buffer
	if err := c.codec.NewEncoder(&body).Encode(m); err != nil {
		return nil, nil, err
	}

	req, err := http.NewRequest("POST", c.url, &body)
	if err != nil {
		return nil, nil, err
	}

	req.Header.Set("Content-Type", c.codec.MediaTypes[0])
	req.Header.Set("Accept", c.codec.MediaTypes[1])

	res, err := c.do(req)
	if err != nil {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		return nil, nil, err
	}

	return c.codec.NewDecoder(res.Body), res.Body, codeErrors[res.StatusCode]
}

// URL returns an Opt that sets a Client's URL.
func URL(rawurl string) Opt { return func(c *Client) { c.url = rawurl } }

// Doer returns an Opt that sets a Client's DoFunc
func Doer(do DoFunc) Opt { return func(c *Client) { c.do = do } }

// Codec returns an Opt that sets a Client's Codec.
func Codec(codec *encoding.Codec) Opt { return func(c *Client) { c.codec = codec } }

// Header returns an Opt that adds a header to an Client's headers.
func Header(k, v string) Opt { return func(c *Client) { c.hdr.Add(k, v) } }
