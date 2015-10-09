package scheduler_test

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

	"github.com/mesos/mesos-go/encoding"
	. "github.com/mesos/mesos-go/scheduler"
)

func TestScheduler_Do(t *testing.T) {
	s := new(Scheduler).With(
		URL(&url.URL{}),
		Codec(encoding.JSONCodec),
		Client(&http.Client{
			Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
				for i, tt := range []struct{ got, want string }{
					{r.Method, "POST"},
					{r.Header.Get("Content-Type"), encoding.JSONCodec.MediaTypes[0]},
					{r.Header.Get("Accept"), encoding.JSONCodec.MediaTypes[1]},
				} {
					if tt.got != tt.want {
						t.Errorf("test #%d: got %v, want %v", i, tt.got, tt.want)
					}
				}
				return response(200), nil
			}),
		}),
	)
	_, _, _ = s.Do(&Call{})

	// TODO(tsenart): Implement mock Mesos scheduler API, verify response codes.
}

func response(code int, body ...byte) *http.Response {
	return &http.Response{
		StatusCode: code,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
