package stream

import (
	"errors"
	"log"
	"net/http"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli"
)

const (
	headerMesosStreamID = "Mesos-Stream-Id"
	debug               = false
)

var errMissingMesosStreamId = errors.New("missing Mesos-Stream-Id header expected with successful SUBSCRIBE")

func Subscribe(cli *httpcli.Client, subscribe encoding.Marshaler) (mesos.Response, httpcli.Opt, error) {
	var mesosStreamID string
	undo := cli.With(httpcli.WrapDoer(func(f httpcli.DoFunc) httpcli.DoFunc {
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
	}))
	defer cli.With(undo) // strip the stream-id grabber
	resp, err := cli.Do(subscribe, httpcli.Close(true))
	streamHeaderOpt := httpcli.DefaultHeader(headerMesosStreamID, mesosStreamID)
	return resp, streamHeaderOpt, err
}
