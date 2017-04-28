package httpcli

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/apierrors"
	"github.com/mesos/mesos-go/api/v1/lib/recordio"
)

// ProtocolError is returned when we receive a response from Mesos that is outside of the HTTP API specification.
// Receipt of the following will yield protocol errors:
//   - any unexpected non-error HTTP response codes (e.g. 199)
//   - any unexpected Content-Type
type ProtocolError string

// Error implements error interface
func (pe ProtocolError) Error() string { return string(pe) }

var defaultErrorMapper = ErrorMapperFunc(apierrors.FromResponse)

const (
	debug = false // TODO(jdef) kill me at some point

	indexRequestContentType  = 0 // index into Client.codec.MediaTypes for request content type
	indexResponseContentType = 1 // index into Client.codec.MediaTypes for expected response content type
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

// Response captures the output of a Mesos HTTP API operation. Callers are responsible for invoking
// Close when they're finished processing the response otherwise there may be connection leaks.
type Response struct {
	io.Closer
	encoding.Decoder
	Header http.Header
}

// ErrorMapperFunc generates an error for the given response.
type ErrorMapperFunc func(*http.Response) error

// ResponseHandler is invoked to process an HTTP response
type ResponseHandler func(*http.Response, error) (mesos.Response, error)

// A Client is a Mesos HTTP APIs client.
type Client struct {
	url            string
	do             DoFunc
	header         http.Header
	codec          *encoding.Codec
	errorMapper    ErrorMapperFunc
	requestOpts    []RequestOpt
	buildRequest   func(encoding.Marshaler, ...RequestOpt) (*http.Request, error)
	handleResponse ResponseHandler
}

// New returns a new Client with the given Opts applied.
// Callers are expected to configure the URL, Do, and Codec options prior to
// invoking Do.
func New(opts ...Opt) *Client {
	c := &Client{
		codec:       &encoding.ProtobufCodec,
		do:          With(DefaultConfigOpt...),
		header:      http.Header{},
		errorMapper: defaultErrorMapper,
	}
	c.buildRequest = c.BuildRequest
	c.handleResponse = c.HandleResponse
	c.With(opts...)
	return c
}

// URL returns the current Mesos API endpoint URL that the caller is set to invoke
func (c *Client) Endpoint() string {
	return c.url
}

// RequestOpt defines a functional option for an http.Request.
type RequestOpt func(*http.Request)

// RequestOpts is a convenience type
type RequestOpts []RequestOpt

// Apply this set of request options to the given HTTP request.
func (opts RequestOpts) Apply(req *http.Request) {
	// apply per-request options
	for _, o := range opts {
		if o != nil {
			o(req)
		}
	}
}

// With applies the given Opts to a Client and returns itself.
func (c *Client) With(opts ...Opt) Opt {
	return Opts(opts).Merged().Apply(c)
}

// WithTemporary configures the Client with the temporary option and returns the results of
// invoking f(). Changes made to the Client by the temporary option are reverted before this
// func returns.
func (c *Client) WithTemporary(opt Opt, f func() error) error {
	if opt != nil {
		undo := c.With(opt)
		defer c.With(undo)
	}
	return f()
}

// Mesos returns a mesos.Client variant backed by this implementation
func (c *Client) Mesos(opts ...RequestOpt) mesos.Client {
	return mesos.ClientFunc(func(m encoding.Marshaler) (mesos.Response, error) {
		return c.Do(m, opts...)
	})
}

// BuildRequest is a factory func that generates and returns an http.Request for the
// given marshaler and request options.
func (c *Client) BuildRequest(m encoding.Marshaler, opt ...RequestOpt) (*http.Request, error) {
	var body bytes.Buffer //TODO(jdef): use a pool to allocate these (and reduce garbage)?
	if err := c.codec.NewEncoder(&body).Encode(m); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", c.url, &body)
	if err != nil {
		return nil, err
	}

	helper := HTTPRequestHelper{req}
	return helper.
		withOptions(c.requestOpts, opt).
		withHeaders(c.header).
		withHeader("Content-Type", c.codec.MediaTypes[indexRequestContentType]).
		withHeader("Accept", c.codec.MediaTypes[indexResponseContentType]).
		Request, nil
}

// HandleResponse parses an HTTP response from a Mesos service endpoint, transforming the
// raw HTTP response into a mesos.Response.
func (c *Client) HandleResponse(res *http.Response, err error) (mesos.Response, error) {
	if err != nil {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		return nil, err
	}

	result := &Response{
		Closer: res.Body,
		Header: res.Header,
	}
	if err = c.errorMapper(res); err != nil {
		return result, err
	}

	switch res.StatusCode {
	case http.StatusOK:
		if debug {
			log.Println("request OK, decoding response")
		}
		ct := res.Header.Get("Content-Type")
		if ct != c.codec.MediaTypes[indexResponseContentType] {
			res.Body.Close()
			return nil, ProtocolError(fmt.Sprintf("unexpected content type: %q", ct))
		}
		result.Decoder = c.codec.NewDecoder(recordio.NewFrameReader(res.Body))

	case http.StatusAccepted:
		if debug {
			log.Println("request Accepted")
		}
		// noop; no decoder for these types of calls

	default:
		return result, ProtocolError(fmt.Sprintf("unexpected mesos HTTP response code: %d", res.StatusCode))
	}

	return result, nil
}

// Do sends a Call and returns (a) a Response (should be closed when finished) that
// contains a streaming Decoder from which callers can read Events from, and; (b) an
// error in case of failure. Callers are expected to *always* close a non-nil Response
// if one is returned. For operations which are successful but also for which there is
// no expected object stream as a result the embedded Decoder will be nil.
func (c *Client) Do(m encoding.Marshaler, opt ...RequestOpt) (res mesos.Response, err error) {
	var req *http.Request
	req, err = c.buildRequest(m, opt...)
	if err == nil {
		res, err = c.handleResponse(c.do(req))
	}
	return
}

// ErrorMapper returns am Opt that overrides the existing error mapping behavior of the client.
func ErrorMapper(em ErrorMapperFunc) Opt {
	return func(c *Client) Opt {
		old := c.errorMapper
		c.errorMapper = em
		return ErrorMapper(old)
	}
}

// URL returns an Opt that sets a Client's URL.
func Endpoint(rawurl string) Opt {
	return func(c *Client) Opt {
		old := c.url
		c.url = rawurl
		return Endpoint(old)
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

// HandleResponse returns a functional config option to set the HTTP response handler of the client.
func HandleResponse(f ResponseHandler) Opt {
	return func(c *Client) Opt {
		old := c.handleResponse
		c.handleResponse = f
		return HandleResponse(old)
	}
}

// RequestOptions returns an Opt that applies the given set of options to every Client request.
func RequestOptions(opts ...RequestOpt) Opt {
	if len(opts) == 0 {
		return nil
	}
	return func(c *Client) Opt {
		old := append([]RequestOpt{}, c.requestOpts...)
		c.requestOpts = opts
		return RequestOptions(old...)
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

// DefaultConfigOpt represents the default client config options.
var DefaultConfigOpt []ConfigOpt

// With returns a DoFunc that executes HTTP round-trips.
// The default implementation provides reasonable defaults for timeouts:
// keep-alive, connection, request/response read/write, and TLS handshake.
// Callers can customize configuration by specifying one or more ConfigOpt's.
func With(opt ...ConfigOpt) DoFunc {
	var (
		dialer = &net.Dialer{
			LocalAddr: &net.TCPAddr{IP: net.IPv4zero},
			KeepAlive: 30 * time.Second,
			Timeout:   5 * time.Second,
		}
		transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial:  dialer.Dial,
			ResponseHeaderTimeout: 5 * time.Second,
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: false},
			TLSHandshakeTimeout:   5 * time.Second,
		}
		config = &Config{
			dialer:    dialer,
			transport: transport,
			client:    &http.Client{Transport: transport},
		}
	)
	for _, o := range opt {
		if o != nil {
			o(config)
		}
	}
	return config.client.Do
}

// Timeout returns an ConfigOpt that sets a Config's response header timeout, tls handshake timeout,
// and dialer timeout.
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

// Transport returns a ConfigOpt that allows tweaks of the default Config's http.Transport
func Transport(modifyTransport func(*http.Transport)) ConfigOpt {
	return func(c *Config) {
		if modifyTransport != nil {
			modifyTransport(c.transport)
		}
	}
}

// WrapRoundTripper allows a caller to customize a configuration's HTTP exchanger. Useful
// for authentication protocols that operate over stock HTTP.
func WrapRoundTripper(f func(http.RoundTripper) http.RoundTripper) ConfigOpt {
	return func(c *Config) {
		if f != nil {
			if rt := f(c.client.Transport); rt != nil {
				c.client.Transport = rt
			}
		}
	}
}

// HTTPRequestHelper wraps an http.Request and provides utility funcs to simplify code elsewhere
type HTTPRequestHelper struct {
	*http.Request
}

func (r *HTTPRequestHelper) withOptions(optsets ...RequestOpts) *HTTPRequestHelper {
	for _, opts := range optsets {
		opts.Apply(r.Request)
	}
	return r
}

func (r *HTTPRequestHelper) withHeaders(hh http.Header) *HTTPRequestHelper {
	for k, v := range hh {
		r.Header[k] = v
		if debug {
			log.Println("request header " + k + ": " + v[0])
		}
	}
	return r
}

func (r *HTTPRequestHelper) withHeader(key, value string) *HTTPRequestHelper {
	r.Header.Set(key, value)
	return r
}
