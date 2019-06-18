package httpsched

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/backoff"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/apierrors"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/calls"
)

const (
	headerMesosStreamID = "Mesos-Stream-Id"
	debug               = false
)

type StateError string

func (err StateError) Error() string { return string(err) }

var (
	errMissingStreamID   = httpcli.ProtocolError("missing Mesos-Stream-Id header expected with successful SUBSCRIBE")
	errAlreadySubscribed = StateError("already subscribed, cannot re-issue a SUBSCRIBE call")
)

type (
	// state implements calls.Caller and tracks connectivity with Mesos
	state struct {
		client      *client // client is a handle to the original underlying HTTP client
		notifyBusy  int32
		notifyQueue chan Notification

		m            sync.Mutex   // m guards the following state:
		fn           phase        // fn is the next state function to execute
		caller       calls.Caller // caller is (maybe) used by a state function to execute a call
		call         *stateCall   // upon executation of fn, this is the most recent call that's been issued
		callCounter  uint64       // index of the most recent call we've issued
		disconnector func()       // disconnector cancels a subscription
		streamID     string       // most recent subscription stream ID returned by mesos
	}

	stateCall struct {
		*scheduler.Call                // call is the next call to execute
		resp            mesos.Response // resp is the Mesos response from the most recently executed call
		err             error          // err is the error from the most recently executed call
		idx             uint64         // captured value of state.callCounter when this object was created
	}

	phase interface {
		isDisconnected() bool
		exec(context.Context, *state) phase
	}

	stateFn           func(context.Context, *state) phase
	disconnectedPhase stateFn
	connectedPhase    stateFn
)

func (disconnectedPhase) isDisconnected() bool { return true }
func (connectedPhase) isDisconnected() bool    { return false }

func (f disconnectedPhase) exec(ctx context.Context, s *state) phase { return f(ctx, s) }
func (f connectedPhase) exec(ctx context.Context, s *state) phase    { return f(ctx, s) }

// DisconnectionDetector is a programmable response decorator that attempts to detect errors
// that should transition the state from "connected" to "disconnected". Detector implementations
// are expected to invoke the `disconnect` callback in order to initiate the disconnection.
//
// The default implementation will transition to a disconnected state when:
//   - an error occurs while decoding an object from the subscription stream
//   - mesos reports an ERROR-type scheduler.Event object via the subscription stream
//   - an object on the stream does not decode to a *scheduler.Event (sanity check)
//
// Consumers of this package may choose to override default behavior by overwriting the default
// value of this var, but should exercise caution: failure to properly transition to a disconnected
// state may cause subsequent Call operations to fail (without recourse).
var DisconnectionDetector = func(disconnect func()) mesos.ResponseDecorator {
	closeF := mesos.CloseFunc(func() (_ error) { disconnect(); return })
	return mesos.ResponseDecoratorFunc(func(resp mesos.Response) mesos.Response {
		return &mesos.ResponseWrapper{
			Response: resp,
			Decoder:  disconnectionDecoder(resp, disconnect),
			Closer:   closeF,
		}
	})
}

func disconnectionDecoder(decoder encoding.Decoder, disconnect func()) encoding.Decoder {
	return encoding.DecoderFunc(func(u encoding.Unmarshaler) (err error) {
		err = decoder.Decode(u)
		if err != nil {
			disconnect()
			return
		}
		switch e := u.(type) {
		case (*scheduler.Event):
			if e.GetType() == scheduler.Event_ERROR {
				// the mesos scheduler API recommends that scheduler implementations
				// resubscribe in this case. we initiate the disconnection here because
				// it is assumed to be convenient for most framework implementations.
				disconnect()
			}
		default:
			// sanity check: this should never happen in practice.
			err = httpcli.ProtocolError(
				fmt.Sprintf("unexpected object on subscription event stream: %v", e))
			disconnect()
		}
		return
	})
}

func doSubscribe(ctx context.Context, ci callerInternal, call *stateCall) (mesosStreamID string, cancel context.CancelFunc) {
	var (
		done            chan struct{} // avoid allocating these chans unless we actually need to redirect
		redirectBackoff <-chan struct{}
		getBackoff      = func(minBackoff, maxBackoff time.Duration) <-chan struct{} {
			if redirectBackoff != nil {
				return redirectBackoff
			}
			done = make(chan struct{})
			redirectBackoff = backoff.Notifier(minBackoff, maxBackoff, done)
			return redirectBackoff
		}
	)
	defer func() {
		if done != nil {
			close(done)
		}
	}()

	// prepare client for a subscription call
	var (
		subscribeOptions = []httpcli.RequestOpt{httpcli.Close(true)}
		subscribeCaller  = &callerTemporary{
			callerInternal: ci,
			requestOpts:    subscribeOptions,
		}
		clearResponse = func() {
			if call.resp != nil {
				call.resp.Close()
			}
			call.resp = nil
		}
	)
	ctx, cancel = context.WithCancel(ctx)
	for attempt := 0; ; attempt++ {
		// execute the call, save the result in resp, err
		call.resp, call.err = subscribeCaller.Call(ctx, call.Call)

		// if err != nil return mustSubscribe since we're unsubscribed
		if call.err != nil {
			clearResponse()
			cancel()
			mesosStreamID = ""
			return
		}

		nmr, ok := call.resp.(*noMasterResponse)
		if !ok {
			break // this is not a redirect
		}

		if attempt >= nmr.maxAttempts {
			call.err = nmr.clientErr
			clearResponse()
			cancel()
			mesosStreamID = ""
			return
		}

		u, err := url.Parse(nmr.newLeaderURL)
		if err != nil {
			call.err = err
			clearResponse()
			cancel()
			mesosStreamID = ""
			return
		}

		subscribeCaller.requestOpts = append(subscribeOptions[:], func(req *http.Request) {
			req.URL = u
		})

		// back off before retrying the subscription attempt
		select {
		case <-getBackoff(nmr.minBackoff, nmr.maxBackoff):
		case <-ctx.Done():
			call.err = ctx.Err()
			clearResponse()
			cancel()
			mesosStreamID = ""
			return
		}
	}

	type mesosStream interface {
		streamID() string
	}

	if sr, ok := call.resp.(mesosStream); ok {
		mesosStreamID = sr.streamID()
	}
	if mesosStreamID == "" {
		clearResponse()
		cancel()
		call.err = errMissingStreamID
	}
	return
}

func mustSubscribe(ctx context.Context, state *state) phase {
	return mustSubscribe0(ctx, state, doSubscribe)
}

func mustSubscribe0(ctx context.Context, state *state, doSubscribe func(context.Context, callerInternal, *stateCall) (string, context.CancelFunc)) phase {
	// (a) validate call = SUBSCRIBE
	if t := state.call.GetType(); t != scheduler.Call_SUBSCRIBE {
		state.call.err = apierrors.CodeUnsubscribed.Error("httpsched: expected SUBSCRIBE instead of " + t.String())
		return disconnectedPhase(mustSubscribe)
	}

	mesosStreamID, cancel := doSubscribe(ctx, state.client, state.call)
	if mesosStreamID == "" {
		cancel()
		return disconnectedPhase(mustSubscribe)
	}

	transitionToDisconnected := func() {
		cancel()

		// Any call can trigger a disconnect.
		// Resubscription attempts block all other calls.
		var phaseChanged bool
		defer func() {
			if phaseChanged {
				state.flushNotify()
			}
		}()

		// NOTE: possibly long-blocking unless assumptions are met.
		state.m.Lock()
		defer state.m.Unlock()

		// Assume that we might be reconnected before this executes. e.g. the same goroutine
		// that initiates the subscription MAY NOT be handling the decoding of individual
		// subscription events.
		if state.streamID == mesosStreamID {
			phaseChanged = state.setPhase(disconnectedPhase(mustSubscribe))
		}
	}

	// wrap the response: any errors processing the subscription stream should result in a
	// transition to a disconnected state ASAP.
	state.call.resp = DisconnectionDetector(func() func() {
		var disconnectOnce sync.Once
		return func() { disconnectOnce.Do(transitionToDisconnected) }
	}()).Decorate(state.call.resp)

	// (e) else prepare callerTemporary w/ special header, return anyCall since we're now subscribed
	state.caller = &callerTemporary{
		callerInternal: state.client,
		requestOpts: []httpcli.RequestOpt{
			httpcli.Header(headerMesosStreamID, mesosStreamID),
		},
	}

	// disconnector probably must be goroutine-safe because it mutates a response that may
	// be concurrently streaming data to a decoder.
	state.disconnector = cancel
	state.streamID = mesosStreamID
	return connectedPhase(anyCall)
}

func errorIndicatesSubscriptionLoss(err error) (result bool) {
	type lossy interface {
		SubscriptionLoss() bool
	}
	if lossyErr, ok := err.(lossy); ok {
		result = lossyErr.SubscriptionLoss()
	}
	return
}

func anyCall(ctx context.Context, state *state) phase {
	// (a) validate call != SUBSCRIBE
	if state.call.GetType() == scheduler.Call_SUBSCRIBE {
		if state.client.allowReconnect {
			// Reset internal state back to DISCONNECTED and re-execute the SUBSCRIBE call.
			// Mesos will hangup on the old SUBSCRIBE socket after this one completes.
			state.caller = nil
			state.call.resp = nil
			state.call.err = nil
			state.disconnector = nil
			state.setPhase(connectedPhase(mustSubscribe))
			return state.fn.exec(ctx, state)
		}

		state.call.resp = nil

		// TODO(jdef) not super happy with this error: I don't think that mesos minds if we issue
		// redundant subscribe calls. However, the state tracking mechanism in this module can't
		// cope with it (e.g. we'll need to track a new stream-id, etc).
		// We make a best effort to transition to a disconnected state if we detect protocol errors,
		// error events, or mesos-generated "not subscribed" errors. But we don't handle things such
		// as, for example, authentication errors. Granted, the initial subscribe call should fail
		// if authentication is an issue, so we should never end up here. I'm not convinced there's
		// not other edge cases though with respect to other error codes.
		state.call.err = errAlreadySubscribed
		return connectedPhase(anyCall)
	}

	// (b) execute call, save the result in resp, err.
	// Release the state lock before issuing a potentially blocking non-SUBSCRIBE call.
	call, caller, disconnector := state.call, state.caller, state.disconnector // pre-unlock state capture

	state.m.Unlock()
	defer state.m.Lock()

	// NOTE: DO NOT reference state.xyz fields that are guarded by the lock!

	call.resp, call.err = caller.Call(ctx, call.Call)

	if errorIndicatesSubscriptionLoss(call.err) {
		// properly transition back to a disconnected state if mesos thinks that we're unsubscribed
		disconnector()
		return disconnectedPhase(mustSubscribe)
	}

	if nmr, ok := call.resp.(*noMasterResponse); ok {
		// properly transition back to a disconnected state if there's been a leadership change
		disconnector()
		call.err = nmr.clientErr
		return disconnectedPhase(mustSubscribe)
	}

	// stay connected, don't attempt to interpret other errors here
	return connectedPhase(anyCall)
}

func (state *state) sendNotify(n Notification) {
	if n != withoutNotification {
		// Preserve ordering of notifications. notifyQueue is buffered and so this is expected
		// to not block under normal conditions.
		state.notifyQueue <- n
	}
}

func (state *state) flushNotify() {
	if !atomic.CompareAndSwapInt32(&state.notifyBusy, 0, 1) {
		return
	}
	defer atomic.StoreInt32(&state.notifyBusy, 0)

	for {
		select {
		case n, ok := <-state.notifyQueue:
			if !ok {
				// Should never happen:
				// 1. This chan is never explicitly closed.
				// 2. All constructors initialize the chan to non-nil.
				return
			}
			state.client.notify(n)
		default:
			return
		}
	}
}

func (state *state) Call(ctx context.Context, oemCall *scheduler.Call) (resp mesos.Response, err error) {
	// Attempt to flush the notification queue after every call.
	defer func() {
		state.flushNotify()
		if debug && err != nil {
			log.Print(*oemCall, err)
		}
	}()

	state.m.Lock()
	defer state.m.Unlock()

	state.callCounter++
	call := &stateCall{Call: oemCall, idx: state.callCounter}
	state.call = call

	// Calls may complete in a different order: we need to ensure that returned stateFn is actually
	// the intended "next-state". we also need to maintain order of notifications that we're sending.
	fn := state.fn.exec(ctx, state)
	if state.callCounter == call.idx {
		state.setPhase(fn)
	} // else, it's an older call so ignore the phase that it returns.

	resp, err = call.resp, call.err
	return
}

// setPhase establishes the next phase func and emits a notification if the connection status changes
// between phases; returns true if a notification was sent.
// requires that the caller is holding the state lock.
func (state *state) setPhase(p phase) bool {
	d1, d2 := state.fn.isDisconnected(), p.isDisconnected()
	state.fn = p

	if d1 == d2 {
		// no connection phase change
		return false
	}
	if d2 {
		// connected -> disconnected
		state.sendNotify(Notification{Type: NotificationDisconnected})
		return true
	}
	// disconnected -> connected
	state.sendNotify(Notification{Type: NotificationConnected})
	return true
}

var withoutNotification = Notification{}
