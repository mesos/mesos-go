package httpsched

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
)

const (
	headerMesosStreamID = "Mesos-Stream-Id"
	debug               = false
)

var (
	errMissingMesosStreamId = errors.New("missing Mesos-Stream-Id header expected with successful SUBSCRIBE")
	errAlreadySubscribed    = errors.New("already subscribed, cannot re-issue a SUBSCRIBE call")
	errNotSubscribed        = errors.New("not yet subscribed, must first issue a SUBSCRIBE call")
)

type (
	// state implements calls.Caller and tracks connectivity with Mesos
	state struct {
		client *client // client is a handle to the original underlying HTTP client

		m      sync.Mutex
		fn     stateFn      // fn is the next state function to execute
		caller calls.Caller // caller is (maybe) used by a state function to execute a call

		call *scheduler.Call // call is the next call to execute
		resp mesos.Response  // resp is the Mesos response from the most recently executed call
		err  error           // err is the error from the most recently executed call
	}

	stateFn func(*state) stateFn
)

func maybeLogged(f httpcli.DoFunc) httpcli.DoFunc {
	if debug {
		return func(req *http.Request) (*http.Response, error) {
			log.Println("wrapping request", req.URL, req.Header)
			resp, err := f(req)
			if err == nil {
				log.Printf("status %d", resp.StatusCode)
				for k := range resp.Header {
					log.Println("header " + k + ": " + resp.Header.Get(k))
				}
			}
			return resp, err
		}
	}
	return f
}

func disconnectedFn(state *state) stateFn {
	// (a) validate call = SUBSCRIBE
	if state.call.GetType() != scheduler.Call_SUBSCRIBE {
		state.resp = nil
		state.err = errNotSubscribed
		return disconnectedFn
	}

	// (b) prepare client for a subscription call
	var (
		mesosStreamID = ""
		undoable      = httpcli.WrapDoer(func(f httpcli.DoFunc) httpcli.DoFunc {
			f = maybeLogged(f)
			return func(req *http.Request) (resp *http.Response, err error) {
				resp, err = f(req)
				if err == nil && resp.StatusCode == 200 {
					// grab Mesos-Stream-Id header; if missing then
					// close the response body and return an error
					mesosStreamID = resp.Header.Get(headerMesosStreamID)
					if mesosStreamID == "" {
						resp.Body.Close()
						resp = nil
						err = errMissingMesosStreamId
					}
				}
				return
			}
		})
		subscribeCaller = &callerTemporary{
			opt:            undoable,
			callerInternal: state.client,
			requestOpts:    []httpcli.RequestOpt{httpcli.Close(true)},
		}
	)

	// (c) execute the call, save the result in resp, err
	stateResp, stateErr := subscribeCaller.Call(state.call)
	state.err = stateErr

	// (d) if err != nil return disconnectedFn since we're unsubscribed
	if stateErr != nil {
		if stateResp != nil {
			stateResp.Close()
		}
		state.resp = nil
		return disconnectedFn
	}

	transitionToDisconnected := func() {
		state.m.Lock()
		defer state.m.Unlock()
		state.fn = disconnectedFn
		_ = stateResp.Close() // swallow any error here
	}

	// wrap the response: any errors processing the subscription stream should result in a
	// transition to a disconnected state ASAP.
	state.resp = &mesos.ResponseWrapper{
		Response: stateResp,
		DecoderFunc: func() encoding.Decoder {
			decoder := stateResp.Decoder()
			return func(u encoding.Unmarshaler) (err error) {
				err = decoder(u)
				if err != nil {
					transitionToDisconnected()
					return
				}
				switch e := u.(type) {
				case (*scheduler.Event):
					if e.GetType() == scheduler.Event_ERROR {
						transitionToDisconnected()
					}
				default:
					err = httpcli.ProtocolError(
						fmt.Sprintf("unexpected object on subscription event stream", e))
				}
				return
			}
		},
	}

	// (e) else prepare callerTemporary w/ special header, return connectedFn since we're now subscribed
	state.caller = &callerTemporary{
		opt:            httpcli.DefaultHeader(headerMesosStreamID, mesosStreamID),
		callerInternal: state.client,
	}
	return connectedFn
}

func connectedFn(state *state) stateFn {
	// (a) validate call != SUBSCRIBE
	if state.call.GetType() == scheduler.Call_SUBSCRIBE {
		state.resp = nil
		state.err = errAlreadySubscribed
		return connectedFn
	}

	// (b) execute call, save the result in resp, err
	state.resp, state.err = state.caller.Call(state.call)

	// stay connected, don't attempt to interpret errors here
	return connectedFn
}

func (state *state) Call(call *scheduler.Call) (resp mesos.Response, err error) {
	state.m.Lock()
	defer state.m.Unlock()
	state.call = call
	state.fn = state.fn(state)

	if debug && state.err != nil {
		log.Print(*call, state.err)
	}

	return state.resp, state.err
}
