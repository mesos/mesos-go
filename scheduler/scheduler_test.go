package scheduler_test

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	. "github.com/mesos/mesos-go/scheduler"
)

func TestScheduler_Do(t *testing.T) {
	t.Fatal("TODO")

	for i, tt := range []struct {
		res *http.Response
		in  *Call
		out *Event
		err error
	}{
		{response(400), &Call{}, nil, errors.New("scheduler: bad status code: 400")},
	} {
		s, err := New(Client(client(tt.res)))
		if err != nil {
			t.Errorf("test #%d: got err: %v", i, err)
		}

		dec, err := s.Do(tt.in)
		if got, want := err, tt.err; !reflect.DeepEqual(got, want) {
			t.Errorf("test #%d: got: %v, want: %v", i, got, want)
		}

		var e Event
		if err := dec.Decode(&e); err != nil {
			t.Errorf("test #%d: got err: %v", i, err)
		} else if got, want := e, tt.out; !reflect.DeepEqual(got, want) {
			t.Errorf("test #%d: got: %v, want: %v", i, got, want)
		}
	}
}

func client(res *http.Response) *http.Client {
	return &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return res, nil
		}),
	}
}

func response(code int, body ...byte) *http.Response {
	return &http.Response{
		StatusCode: code,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
