package httpsched

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/extras/latch"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func TestDisconnectionDecoder(t *testing.T) {

	// invoke disconnect upon decoder errors
	expected := errors.New("unmarshaler error")
	decoder := encoding.DecoderFunc(func(_ encoding.Unmarshaler) error { return expected })
	latch := new(latch.L).Reset()

	d := disconnectionDecoder(decoder, latch.Close)
	err := d.Decode(nil)
	if err != expected {
		t.Errorf("expected %v instead of %v", expected, err)
	}
	if !latch.Closed() {
		t.Error("disconnect func was not called")
	}

	// ERROR event triggers disconnect
	latch.Reset()
	errtype := scheduler.Event_ERROR
	event := &scheduler.Event{Type: errtype}
	decoder = encoding.DecoderFunc(func(um encoding.Unmarshaler) error { return nil })
	d = disconnectionDecoder(decoder, latch.Close)
	_ = d.Decode(event)
	if !latch.Closed() {
		t.Error("disconnect func was not called")
	}

	// sanity: non-ERROR event does not trigger disconnect
	latch.Reset()
	errtype = scheduler.Event_SUBSCRIBED
	event = &scheduler.Event{Type: errtype}
	_ = d.Decode(event)
	if latch.Closed() {
		t.Error("disconnect func was unexpectedly called")
	}

	// non scheduler.Event objects trigger disconnect
	latch.Reset()
	nonEvent := &scheduler.Call{}
	_ = d.Decode(nonEvent)
	if !latch.Closed() {
		t.Error("disconnect func was not called")
	}
}

func TestMustSubscribe(t *testing.T) {
	subscribeCall := &scheduler.Call{Type: scheduler.Call_SUBSCRIBE}
	type subscription struct {
		resp   mesos.Response
		cancel context.CancelFunc
	}
	newSubscription := func(err error) subscription {
		closed := make(chan struct{})
		var closeOnce sync.Once
		cancel := func() { closeOnce.Do(func() { close(closed) }) }
		resp := &mesos.ResponseWrapper{
			Decoder: encoding.DecoderFunc(func(encoding.Unmarshaler) (_ error) {
				if err == context.Canceled {
					select {
					case <-closed:
						return err
					default:
						return
					}
				}
				if err != nil {
					return err
				}
				return
			}),
			Closer: mesos.CloseFunc(func() (_ error) { cancel(); return }),
		}
		return subscription{
			cancel: cancel,
			resp:   resp,
		}
	}
	for ti, tc := range map[string]struct {
		state    *state
		streamID string
		sub      subscription
		un       encoding.Unmarshaler
		//-- wants:
		wantsDisconnected bool
	}{
		"<>": {
			state:             &state{call: &stateCall{}},
			sub:               subscription{cancel: func() {}},
			wantsDisconnected: true,
		},
		"subFailed": {
			state:             &state{call: &stateCall{Call: subscribeCall}},
			sub:               subscription{cancel: func() {}},
			wantsDisconnected: true,
		},
		"subWorkedDecoderCanceled": {
			state: &state{
				call:        &stateCall{Call: subscribeCall},
				client:      &client{},
				notifyQueue: make(chan Notification, 1)},
			streamID: "1",
			sub:      newSubscription(context.Canceled),
			un:       &scheduler.Event{},
			// response decoder will not return context canceled unless the disconnector has been invoked
		},
		"subWorkedDecoderDeadlineExceeded": {
			state: &state{
				call:        &stateCall{Call: subscribeCall},
				client:      &client{},
				notifyQueue: make(chan Notification, 1)},
			streamID: "1",
			sub:      newSubscription(context.DeadlineExceeded),
		},
		"subWorkedDecoderBadObject": {
			state: &state{
				call:        &stateCall{Call: subscribeCall},
				client:      &client{},
				notifyQueue: make(chan Notification, 1)},
			streamID: "1",
			sub:      newSubscription(nil),
			un:       &scheduler.Call{},
		},
		"subWorkedDecoderSchedulerError": {
			state: &state{
				call:        &stateCall{Call: subscribeCall},
				client:      &client{},
				notifyQueue: make(chan Notification, 1)},
			streamID: "1",
			sub:      newSubscription(nil),
			un:       &scheduler.Event{Type: scheduler.Event_ERROR},
		},
	} {
		t.Run(ti, func(t *testing.T) {
			p := mustSubscribe0(context.Background(), tc.state,
				func(_ context.Context, _ callerInternal, cl *stateCall) (string, context.CancelFunc) {
					cl.resp = tc.sub.resp
					return tc.streamID, tc.sub.cancel
				})
			if tc.wantsDisconnected != p.isDisconnected() {
				if tc.wantsDisconnected {
					t.Fatal("unexpectedly disconnected")
				}
				t.Fatal("expected to be disconnected but was not")
			}
			if tc.wantsDisconnected {
				return
			}

			// check that state disconnector() doesn't change the phase
			tc.state.fn = p
			tc.state.disconnector()
			if tc.state.fn.isDisconnected() {
				t.Fatal("disconnector() incorrectly transitioned phase to disconnected")
			}
			if tc.state.streamID != tc.streamID {
				t.Fatalf("expected stream %q instead of %q", tc.streamID, tc.state.streamID)
			}

			// fake response whose decoder disconnects for various reasons
			err := tc.state.call.resp.Decode(tc.un)
			if _, ok := tc.un.(*scheduler.Event); err == nil && !ok {
				t.Fatal("expected error but got none")
			}
			t.Log(err)
			if !tc.state.fn.isDisconnected() {
				t.Fatal("disconnector() incorrectly transitioned phase to disconnected")
			}
		})
	}
}
