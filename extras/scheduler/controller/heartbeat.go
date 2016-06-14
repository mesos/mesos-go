package controller

import (
	"errors"
	"time"

	"github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/scheduler/events"
)

// ErrMissedHeartbeats is returned after the scheduler misses too many heartbeat events from Mesos master.
var ErrMissedHeartbeats = errors.New("missed too many subsequent Mesos master heartbeats")

type MonitorContext interface {
	// Errors is a sink for heartbeat-related errors.
	// NOTE: a closed Errors chan may generate a panic, even if the Done chan is already
	// closed. Instead it is safer for this func to return nil.
	Errors() chan<- error
	// Done should be closed to indicate that a heartbeat monitor should exit. MUST NOT
	// return nil.
	Done() <-chan struct{}
}

// HeartbeatMonitor generates an events.Handler decorator that monitors the incoming event stream
// from Mesos and generates an error if too much time passes without receiving events from Mesos.
// Mesos is expected to generate regular HEARTBEAT events in order to allow scheduler's to more easily
// detect network parititions. The only kind of error sent to ctx.Errors is ErrMissedHeartbeats.
func HeartbeatMonitor(ctx MonitorContext, missedHeartbeatThreshold int) events.Decorator {
	return func(h events.Handler) events.Handler {
		state := &monitorState{ctx, missedHeartbeatThreshold, nil}
		return events.Handlers{
			state.maybeInitHeartbeatMonitor(),
			state.maybeSendHeartbeat(),
			h, // and LAST delegate event handling to the decorated handler
		}
	}
}

type monitorState struct {
	context                  MonitorContext
	missedHeartbeatThreshold int
	heartbeats               chan struct{}
}

func (state *monitorState) maybeInitHeartbeatMonitor() events.Handler {
	return events.When(scheduler.Event_SUBSCRIBED, events.OnceFunc(func(e *scheduler.Event) error {
		if i := e.GetSubscribed().HeartbeatIntervalSeconds; i != nil {
			state.heartbeats = make(chan struct{})
			go state.runMonitor(newTimerMonitor(time.Duration(*i*float64(time.Second)), state.heartbeats))
		}
		return nil
	}))
}

func (state *monitorState) maybeSendHeartbeat() events.Handler {
	return events.WhenFunc(events.PredicateBool(func() bool { return state.heartbeats != nil }), func(_ *scheduler.Event) error {
		// all other event types signal that we're still connected to the master so send a heartbeat.
		// it's important that the heartbeat listener NOT block for very long since this blocks the
		// entire event processing chain.
		select {
		case state.heartbeats <- struct{}{}:
		case <-state.context.Done():
		}
		return nil
	})
}

// runMonitor generates an ErrMissedHeartbeats error upon missing some number of subsequent heartbeat signals.
// blocks until the MonitorContext.Done chan is closed. Upon returning the heartrateMonitor is stopped.
func (state *monitorState) runMonitor(hm heartrateMonitor) {
	missed := 0
	defer hm.Stop()
	for {
		select {
		case <-hm.Heartbeats():
			missed = 0
		case <-hm.Alarm():
			missed++
			if missed > state.missedHeartbeatThreshold {
				select {
				case state.context.Errors() <- ErrMissedHeartbeats:
				case <-state.context.Done():
					return
				}
			}
		case <-state.context.Done():
			return
		}
		hm.Reset()
	}
}

type (
	heartrateMonitor interface {
		Reset()                      // Reset restarts the heartbeat-watch-timer
		Stop()                       // Stop terminates the monitor and releases resources
		Alarm() <-chan time.Time     // Alarm yields a timestamp when some number of heartbeats have been missed
		Heartbeats() <-chan struct{} // Heartbeats yields a struct upon the occurrance of some events received from Mesos
	}

	// timerMonitor is the time.Timer based heartrate monitor
	timerMonitor struct {
		timer             *time.Timer
		heartbeatInterval time.Duration
		heartbeats        <-chan struct{}
	}
)

func (tm *timerMonitor) Reset()                      { tm.timer.Reset(tm.heartbeatInterval) }
func (tm *timerMonitor) Stop()                       { tm.timer.Stop() }
func (tm *timerMonitor) Alarm() <-chan time.Time     { return tm.timer.C }
func (tm *timerMonitor) Heartbeats() <-chan struct{} { return tm.heartbeats }

func newTimerMonitor(d time.Duration, heartbeats <-chan struct{}) heartrateMonitor {
	return &timerMonitor{time.NewTimer(d), d, heartbeats}
}
