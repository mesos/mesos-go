package stream

import (
	"errors"
	"io"
	"net/http"

	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli"
)

const headerMesosStreamID = "Mesos-Stream-Id"

var errMissingMesosStreamId = errors.New("missing Mesos-Stream-Id header expected with successful SUBSCRIBE")

func Subscribe(cli *httpcli.Client, subscribe encoding.Marshaler) (encoding.Decoder, io.Closer, httpcli.Opt, error) {
	var mesosStreamID string
	undo := cli.With(httpcli.WrapDoer(func(f httpcli.DoFunc) httpcli.DoFunc {
		return func(req *http.Request) (*http.Response, error) {
			resp, err := f(req)
			if err == nil && resp.StatusCode == 200 {
				// grab Mesos-Stream-Id header; if missing then
				// close the response body and return an error
				mesosStreamID = resp.Header.Get(headerMesosStreamID)
				if mesosStreamID == "" {
					resp.Body.Close()
					return nil, errMissingMesosStreamId
				}
			}
			return resp, err
		}
	}))
	defer cli.With(undo) // strip the stream-id grabber
	eventDecoder, conn, err := cli.Do(subscribe, httpcli.Close(true))
	streamHeaderOpt := httpcli.DefaultHeader(headerMesosStreamID, mesosStreamID)
	return eventDecoder, conn, streamHeaderOpt, err
}
