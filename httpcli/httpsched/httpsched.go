package httpsched

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/backoff"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli"
)

const (
	headerMesosStreamID = "Mesos-Stream-Id"
	debug               = false
)

var (
	errMissingMesosStreamId = errors.New("missing Mesos-Stream-Id header expected with successful SUBSCRIBE")
	errNotHTTP              = errors.New("expected an HTTP object, found something else instead")

	// MinRedirectBackoffPeriod MUST be set to some non-zero number, otherwise redirects will panic
	MinRedirectBackoffPeriod = 100 * time.Millisecond
	// MaxRedirectBackoffPeriod SHOULD be set to a value greater than MinRedirectBackoffPeriod
	MaxRedirectBackoffPeriod = 13 * time.Second
)

type (
	client struct {
		*httpcli.Client
	}

	Client interface {
		// CallNoData is for scheduler calls that are not expected to return any data from the server.
		CallNoData(encoding.Marshaler) error
		// Subscribe issues a SUBSCRIBE call to Mesos and properly manages the Mesos-Stream-Id header in the response.
		Subscribe(encoding.Marshaler) (mesos.Response, httpcli.Opt, error)
		// WithTemporary configures the Client with the temporary option and returns the results of
		// invoking f(). Changes made to the Client by the temporary option are reverted before this
		// func returns.
		WithTemporary(opt httpcli.Opt, f func() error) error
	}
)

// NewClient returns a scheduler API Client
func NewClient(cl *httpcli.Client) Client { return &client{Client: cl} }

// CallNoData implements Client
func (cli *client) CallNoData(call encoding.Marshaler) error {
	resp, err := cli.callWithRedirect(func() (mesos.Response, error) {
		return cli.Do(call)
	})
	if resp != nil {
		resp.Close()
	}
	return err
}

// Subscribe implements Client
func (cli *client) Subscribe(subscribe encoding.Marshaler) (resp mesos.Response, maybeOpt httpcli.Opt, subscribeErr error) {
	var (
		mesosStreamID = ""
		opt           = httpcli.WrapDoer(func(f httpcli.DoFunc) httpcli.DoFunc {
			return func(req *http.Request) (*http.Response, error) {
				if debug {
					log.Println("wrapping request")
				}
				resp, err := f(req)
				if debug && err == nil {
					log.Printf("status %d", resp.StatusCode)
					for k := range resp.Header {
						log.Println("header " + k + ": " + resp.Header.Get(k))
					}
				}
				if err == nil && resp.StatusCode == 200 {
					// grab Mesos-Stream-Id header; if missing then
					// close the response body and return an error
					mesosStreamID = resp.Header.Get(headerMesosStreamID)
					if mesosStreamID == "" {
						resp.Body.Close()
						return nil, errMissingMesosStreamId
					}
					if debug {
						log.Println("found mesos-stream-id: " + mesosStreamID)
					}
				}
				return resp, err
			}
		})
	)
	cli.WithTemporary(opt, func() error {
		resp, subscribeErr = cli.callWithRedirect(func() (mesos.Response, error) {
			return cli.Do(subscribe, httpcli.Close(true))
		})
		return nil
	})
	maybeOpt = httpcli.DefaultHeader(headerMesosStreamID, mesosStreamID)
	return
}

func (cli *client) callWithRedirect(f func() (mesos.Response, error)) (resp mesos.Response, err error) {
	var (
		done            chan struct{} // avoid allocating these chans unless we actually need to redirect
		redirectBackoff <-chan struct{}
		getBackoff      = func() <-chan struct{} {
			if redirectBackoff == nil {
				done = make(chan struct{})
				redirectBackoff = backoff.Notifier(MinRedirectBackoffPeriod, MaxRedirectBackoffPeriod, done)
			}
			return redirectBackoff
		}
	)
	defer func() {
		if done != nil {
			close(done)
		}
	}()
	for attempt := 0; ; attempt++ {
		resp, err = f()
		if err == nil || (err != nil && err != httpcli.ErrNotLeader) {
			return resp, err
		}
		res, ok := resp.(*httpcli.Response)
		if !ok {
			if resp != nil {
				resp.Close()
			}
			return nil, errNotHTTP
		}
		// TODO(jdef) refactor this
		// mesos v0.29 will actually send back fully-formed URLs in the Location header
		log.Println("master changed?")
		if attempt < cli.MaxRedirects() {
			location, ok := buildNewEndpoint(res.Header.Get("Location"), cli.URL())
			if !ok {
				return
			}
			res.Close()
			log.Println("redirecting to " + location)
			cli.With(httpcli.URL(location))
			<-getBackoff()
			continue
		}
		return
	}
}

func buildNewEndpoint(location, currentEndpoint string) (string, bool) {
	if location == "" {
		return "", false
	}
	// current format appears to be //x.y.z.w:port
	hostport, parseErr := url.Parse(location)
	if parseErr != nil || hostport.Host == "" {
		return "", false
	}
	current, parseErr := url.Parse(currentEndpoint)
	if parseErr != nil {
		return "", false
	}
	current.Host = hostport.Host
	return current.String(), true
}
