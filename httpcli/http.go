package httpcli

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/recordio"
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

const debug = false

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

// Response captures the output of a Mesos HTTP API operation. Callers are responsible for invoking
// Close when they're finished processing the response otherwise there may be connection leaks.
type Response struct {
	io.Closer
	StatusCode int
	Header     http.Header
	Decoder    encoding.Decoder
}

// A Client is a Mesos HTTP APIs client.
type Client struct {
	url    string
	do     DoFunc
	header http.Header
	codec  *encoding.Codec
}

// New returns a new Client with the given Opts applied.
// Callers are expected to configure the URL, Do, and Codec options prior to
// invoking Do.
func New(opts ...Opt) *Client {
	c := &Client{
		codec:  &encoding.ProtobufCodec,
		do:     With(),
		header: http.Header{},
	}
	c.With(opts...)
	return c
}

// Opt defines a functional option for the HTTP client type. A functional option
// must return an Opt that acts as an "undo" if applied to the same Client.
type Opt func(*Client) Opt

// RequestOpt defines a functional option for an http.Request.
type RequestOpt func(*http.Request)

// With applies the given Opts to a Client and returns itself.
func (c *Client) With(opts ...Opt) Opt {
	var noop Opt
	noop = Opt(func(_ *Client) Opt { return noop })

	last := noop
	for _, opt := range opts {
		if opt != nil {
			last = opt(c)
		}
	}
	return last
}

// Do sends a Call and returns a streaming Decoder from which callers can read
// Events from, an io.Closer to close the event stream on graceful termination
// and an error in case of failure. Callers are expected to *always* close a
// non-nil io.Closer if one is returned. For operations which are successful
// but also for which there is no expected object stream as a result the
// returned Decoder will be nil.
func (c *Client) Do(m encoding.Marshaler, opt ...RequestOpt) (*Response, error) {
	var body bytes.Buffer
	if err := c.codec.NewEncoder(&body).Invoke(m); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", c.url, &body)
	if err != nil {
		return nil, err
	}

	// default headers, applied to all requests
	for k, v := range c.header {
		req.Header[k] = v
		if debug {
			log.Println("request header " + k + ": " + v[0])
		}
	}

	// apply per-request options
	for _, o := range opt {
		if o != nil {
			o(req)
		}
	}

	// these headers override anything that a caller may have tried to set
	req.Header.Set("Content-Type", c.codec.MediaTypes[0])
	req.Header.Set("Accept", c.codec.MediaTypes[1])

	res, err := c.do(req)
	if err != nil {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		return nil, err
	}

	var events encoding.Decoder
	switch res.StatusCode {
	case http.StatusOK:
		events = c.codec.NewDecoder(recordio.NewFrameReader(res.Body))
	case http.StatusAccepted:
		// noop; no data to decode for these types of calls
	case http.StatusTemporaryRedirect:
		// TODO(jdef) refactor this
		// mesos v0.29 will actually send back fully-formed URLs in the Location header
		if debug {
			log.Println("master changed!")
		}
		newMaster := res.Header.Get("Location")
		if newMaster != "" {
			// current format appears to be //x.y.z.w:port
			hostport, parseErr := url.Parse(newMaster)
			if parseErr == nil && hostport.Host != "" {
				current, parseErr := url.Parse(c.url)
				if parseErr == nil {
					current.Host = hostport.Host
					c.url = current.String()
					if debug {
						log.Println("master changed, redirecting to " + c.url)
					}
					res.Body.Close()
					return c.Do(m, opt...)
				}
			}
		}
	default:
		if _, found := codeErrors[res.StatusCode]; !found {
			// TODO(jdef) this is terrible, but handy during development; we should probably just
			// return a ProtocolError or something
			panic("unexpected status code " + strconv.Itoa(int(res.StatusCode)))
		}
	}
	return &Response{
		StatusCode: res.StatusCode,
		Header:     res.Header,
		Decoder:    events,
		Closer:     res.Body,
	}, codeErrors[res.StatusCode]
}

// URL returns an Opt that sets a Client's URL.
func URL(rawurl string) Opt {
	return func(c *Client) Opt {
		old := c.url
		c.url = rawurl
		return URL(old)
	}
}

// WrapDoer returns an Opt that decorates a Client's DoFunc
func WrapDoer(f func(DoFunc) DoFunc) Opt {
	return func(c *Client) Opt {
		old := c.do
		c.do = f(c.do)
		return Do(old)
	}
}

// Do returns an Opt that sets a Client's DoFunc
func Do(do DoFunc) Opt {
	return func(c *Client) Opt {
		old := c.do
		c.do = do
		return Do(old)
	}
}

// Codec returns an Opt that sets a Client's Codec.
func Codec(codec *encoding.Codec) Opt {
	return func(c *Client) Opt {
		old := c.codec
		c.codec = codec
		return Codec(old)
	}
}

// DefaultHeader returns an Opt that adds a header to an Client's headers.
func DefaultHeader(k, v string) Opt {
	return func(c *Client) Opt {
		old, found := c.header[k]
		old = append([]string{}, old...) // clone
		c.header.Add(k, v)
		return func(c *Client) Opt {
			if found {
				c.header[k] = old
			} else {
				c.header.Del(k)
			}
			return DefaultHeader(k, v)
		}
	}
}

// Header returns an RequestOpt that adds a header value to an HTTP requests's header.
func Header(k, v string) RequestOpt { return func(r *http.Request) { r.Header.Add(k, v) } }

// Close returns a RequestOpt that determines whether to close the underlying connection after sending the request.
func Close(b bool) RequestOpt { return func(r *http.Request) { r.Close = b } }

type Config struct {
	client    *http.Client
	dialer    *net.Dialer
	transport *http.Transport
}

type ConfigOpt func(*Config)

// With returns a DoFunc that executes HTTP round-trips.
// The default implementation provides reasonable defaults for timeouts:
// keep-alive, connection, request/response read/write, and TLS handshake.
// Callers can customize configuration by specifying one or more ConfigOpt's.
func With(opt ...ConfigOpt) DoFunc {
	dialer := &net.Dialer{
		LocalAddr: &net.TCPAddr{IP: net.IPv4zero},
		KeepAlive: 30 * time.Second,
		Timeout:   5 * time.Second,
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		Dial:  dialer.Dial,
		ResponseHeaderTimeout: 5 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: false},
		TLSHandshakeTimeout:   5 * time.Second,
	}
	config := &Config{
		dialer:    dialer,
		transport: transport,
		client:    &http.Client{Transport: transport},
	}
	for _, o := range opt {
		if o != nil {
			o(config)
		}
	}
	return config.client.Do
}

// Timeout returns an ConfigOpt that sets a Config's timeout and keep-alive timeout.
func Timeout(d time.Duration) ConfigOpt {
	return func(c *Config) {
		c.transport.ResponseHeaderTimeout = d
		c.transport.TLSHandshakeTimeout = d
		c.dialer.Timeout = d
	}
}

// RoundTripper returns a ConfigOpt that sets a Config's round-tripper.
func RoundTripper(rt http.RoundTripper) ConfigOpt {
	return func(c *Config) {
		c.client.Transport = rt
	}
}

// TLSConfig returns a ConfigOpt that sets a Config's TLS configuration.
func TLSConfig(tc *tls.Config) ConfigOpt {
	return func(c *Config) {
		c.transport.TLSClientConfig = tc
	}
}
