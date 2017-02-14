package httpsched

import (
	"errors"
	"log"
	"net/http"
	"sync"

	"github.com/mesos/mesos-go/api/v1/lib"
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
	state.resp, state.err = subscribeCaller.Call(state.call)

	// (d) if err != nil return unsubscribedFn
	if state.err != nil {
		return disconnectedFn
	}

	// (e) else prepare callerTemporary w/ special header, return subscribingFn
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

	// (c) return connectedFn; TODO(jdef) detect specific Mesos error codes as triggers -> disconnectedFn?
	return connectedFn
}

func (state *state) Call(call *scheduler.Call) (resp mesos.Response, err error) {
	state.m.Lock()
	defer state.m.Unlock()
	state.call = call
	state.fn = state.fn(state)
	return state.resp, state.err
}
