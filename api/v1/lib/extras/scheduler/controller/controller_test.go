package controller

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/encoding"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler/events"
)

const patience = 10 * time.Second

var (
	eof           = errors.New("eof")
	tooManyEvents = errors.New("too many events")
)

func TestEventLoop(t *testing.T) {
	type action func(cancel context.CancelFunc, decoder chan<- struct{})
	for i, tc := range []struct {
		action   action
		wantsErr error
	}{
		{
			action:   func(cancel context.CancelFunc, _ chan<- struct{}) { cancel() },
			wantsErr: context.Canceled,
		},
		{
			action:   func(_ context.CancelFunc, d chan<- struct{}) { close(d) },
			wantsErr: eof,
		},
		{
			action: func(_ context.CancelFunc, d chan<- struct{}) {
				select {
				case d <- struct{}{}:
				case <-time.After(patience):
					t.Log("timed out trying to send 2nd event")
				}
			},
			wantsErr: tooManyEvents,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			decoded := make(chan struct{})
			d := encoding.DecoderFunc(func(encoding.Unmarshaler) error {
				select {
				case _, ok := <-decoded:
					if !ok {
						return eof
					}
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})

			numEvents := 0
			h := events.HandlerFunc(func(context.Context, *scheduler.Event) error {
				numEvents++
				if numEvents > 1 {
					return tooManyEvents
				}
				return nil
			})

			ch := make(chan error, 1)
			go func() {
				defer close(ch)
				err := eventLoop(ctx, Config{handler: h}, d)
				ch <- err
			}()

			select {
			case decoded <- struct{}{}:
			case <-time.After(patience):
				t.Fatal("timed out trying to send event via decoder")
			}

			tc.action(cancel, decoded) // termination event

			select {
			case err := <-ch:
				if err != tc.wantsErr {
					t.Fatalf("unexpected error state: %v", err)
				}
				expectedEvents := 1
				if err == tooManyEvents {
					expectedEvents++
				}
				if numEvents != expectedEvents {
					t.Fatalf("expected %d event(s) instead of %d", expectedEvents, numEvents)
				}
			case <-time.After(patience):
				t.Fatal("timed out waiting for event loop to exit")
			}
		})
	}
}

func TestProcessSubscription(t *testing.T) {
	t.Run("default", func(t *testing.T) { testProcessSubscription(t, false) })
	t.Run("ctxPerSub", func(t *testing.T) { testProcessSubscription(t, true) })
}

func testProcessSubscription(t *testing.T, contextPerSubscription bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	decoded := make(chan struct{})
	d := encoding.DecoderFunc(func(encoding.Unmarshaler) error {
		select {
		case _, ok := <-decoded:
			if !ok {
				return eof
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	numEvents := 0
	var eventCtx context.Context
	h := events.HandlerFunc(func(ctx context.Context, _ *scheduler.Event) error {
		numEvents++
		if numEvents > 1 {
			return tooManyEvents
		}
		eventCtx = ctx
		return nil
	})

	ch := make(chan error, 1)
	go func() {
		defer close(ch)
		err := processSubscription(ctx,
			Config{
				handler:                h,
				contextPerSubscription: contextPerSubscription,
			},
			&mesos.ResponseWrapper{Decoder: d},
			nil,
		)
		ch <- err
	}()

	select {
	case decoded <- struct{}{}:
	case <-time.After(patience):
		t.Fatal("timed out trying to send event via decoder")
	}

	close(decoded) // termination event

	select {
	case err := <-ch:
		if err != eof {
			t.Fatalf("unexpected error state: %v", err)
		}
		expectedEvents := 1
		if err == tooManyEvents {
			expectedEvents++
		}
		if numEvents != expectedEvents {
			t.Fatalf("expected %d event(s) instead of %d", expectedEvents, numEvents)
		}
	case <-time.After(patience):
		t.Fatal("timed out waiting for event loop to exit")
	}

	select {
	case <-eventCtx.Done():
		if !contextPerSubscription {
			t.Fatal("unexpected canceled event context")
		}
	default:
		if contextPerSubscription {
			t.Fatal("expected canceled event context")
		}
	}
}
