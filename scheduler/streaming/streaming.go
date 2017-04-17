package streaming

import (
	"errors"
	"sync"

	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
)

type Stream struct {
	in                  chan interface{} // chan that contains event objects from scheduler.Scheduler callbacks
	out                 chan interface{} // chan that contains filtered event objects published by state funcs
	shutdown            chan struct{}    // chan that closes to signal that the state machine should stop
	err                 error            // non-nil if this stream receives an EventError, or the driver aborts
	errOnce             sync.Once        // only set err once
	connectedStateFn    StateFn          // customizable "connected" state func
	disconnectedStateFn StateFn          // customizable "disconnected" state func
}

// StateFn implemenations must return nil if they read an EventError object from Stream.in, otherwise
// they are expected to read an EventXYZ object from the given chan and act upon it, returning a non-nil
// state func as long as the state machine should stay running.
type StateFn func(*Stream, <-chan interface{}) StateFn

func newStream(disStateFn, conStateFn StateFn) *Stream {
	s := &Stream{
		in:  make(chan interface{}),
		out: make(chan interface{}),
	}
	if disStateFn != nil && conStateFn != nil {
		s.disconnectedStateFn = disStateFn
		s.connectedStateFn = conStateFn
	} else if disStateFn == nil && conStateFn == nil {
		s.disconnectedStateFn = defaultDisconnectedStateFn
		s.connectedStateFn = defaultConnectedStateFn
		s.shutdown = make(chan struct{})
	} else {
		panic("caller must specify both disconnect and connect state funcs, or neither")
	}
	return s
}

// Events returns a chan that produces state-func-filtered EventXYZ objects; it chan closes when the underlying state
// machine has terminated. See Stream.Run().
func (s *Stream) Events() <-chan interface{} {
	return s.out
}

// Error returns the current error state of the stream.
func (s *Stream) Error() error {
	return s.err
}

func defaultConnectedStateFn(s *Stream, in <-chan interface{}) StateFn {
	select {
	case <-s.shutdown:
		return nil

	case event, ok := <-in:
		if !ok {
			// closed events chan indicates termination time
			return nil
		}
		switch event := event.(type) {
		default:
			s.out <- event
		case EventDisconnected:
			s.out <- event
			return defaultDisconnectedStateFn
		case EventError:
			// unrecoverable error
			s.errOnce.Do(func() { s.err = errors.New(event.Message) })
			s.out <- event
			return nil
		}
		return defaultConnectedStateFn
	}
}

func defaultDisconnectedStateFn(s *Stream, in <-chan interface{}) StateFn {
	select {
	case <-s.shutdown:
		return nil

	case event, ok := <-in:
		if !ok {
			// closed events chan indicates termination time
			return nil
		}
		switch event := event.(type) {
		default:
			// ignore other event types
			return defaultDisconnectedStateFn
		case EventError:
			// unrecoverable error
			s.errOnce.Do(func() { s.err = errors.New(event.Message) })
			s.out <- event
			return nil
		case EventRegistered:
			s.out <- event
		case EventReregistered:
			s.out <- event
		}
		return defaultConnectedStateFn
	}
}

// Run executes a state machine that returns when the next state is nil; clients are expected to
// execute this func in a go-routine. Upon exiting Stream.out chan is closed to indicate
// termination of the state machine. Should only be invoked once per the lifetime of the Stream.
func (s *Stream) Run() {
	defer close(s.out)
	state := s.disconnectedStateFn
	for state != nil {
		state = state(s, s.in)
	}
}

// New creates a internal scheduler adapter that will stream scheduler.Scheduler callbacks as events.
// The caller is responsible for starting and then monitoring the returned scheduler.SchedulerDriver,
// as well as invoking Stream.Run.
func New(config scheduler.DriverConfig, disStateFn, conStateFn StateFn) (*Stream, scheduler.SchedulerDriver, error) {
	s := newStream(disStateFn, conStateFn)
	config.Scheduler = &adapter{Stream: s}
	driver, err := scheduler.NewMesosSchedulerDriver(config)
	if err != nil {
		return nil, nil, err
	}
	return s, driver, nil
}

// NewSingleUse creates and then starts a new scheduler.SchedulerDriver using the given config
// and returns a Stream object that yields event objects until the underlying driver aborts or
// is terminated.
func NewSingleUse(config scheduler.DriverConfig) (*Stream, error) {
	s, driver, err := New(config, nil, nil)
	if err != nil {
		return nil, err
	}
	status, err := driver.Start()
	if err != nil {
		return nil, err
	}
	if status != mesos.Status_DRIVER_RUNNING {
		return nil, errors.New("failed to start scheduler driver")
	}
	go s.Run()
	go func() {
		defer close(s.shutdown)
		_, err := driver.Join()
		s.errOnce.Do(func() { s.err = err })
	}()
	return s, nil
}
