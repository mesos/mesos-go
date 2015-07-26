package streaming

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
)

// adapter struct implements scheduler.Scheduler callbacks and transforms
// them into EventXYZ objects, writing the resulting objects to Stream.in.
type adapter struct {
	*Stream
}

func (a *adapter) Registered(driver scheduler.SchedulerDriver, fi *mesos.FrameworkID, mi *mesos.MasterInfo) {
	event := EventRegistered{
		EventMeta:   EventMeta{Driver: driver},
		FrameworkID: fi.GetValue(),
		MasterInfo:  mi,
	}
	a.Stream.in <- event
}

func (a *adapter) Reregistered(driver scheduler.SchedulerDriver, mi *mesos.MasterInfo) {
	event := EventReregistered{
		EventMeta:  EventMeta{Driver: driver},
		MasterInfo: mi,
	}
	a.Stream.in <- event
}

func (a *adapter) Disconnected(driver scheduler.SchedulerDriver) {
	event := EventDisconnected{
		EventMeta: EventMeta{Driver: driver},
	}
	a.Stream.in <- event
}

func (a *adapter) ResourceOffers(driver scheduler.SchedulerDriver, offers []*mesos.Offer) {
	event := EventResourceOffers{
		EventMeta: EventMeta{Driver: driver},
		Offers:    offers,
	}
	a.Stream.in <- event
}

func (a *adapter) OfferRescinded(driver scheduler.SchedulerDriver, offerID *mesos.OfferID) {
	event := EventOfferRescinded{
		EventMeta: EventMeta{Driver: driver},
		OfferID:   offerID.GetValue(),
	}
	a.Stream.in <- event
}

func (a *adapter) StatusUpdate(driver scheduler.SchedulerDriver, status *mesos.TaskStatus) {
	event := EventStatusUpdate{
		EventMeta:  EventMeta{Driver: driver},
		TaskStatus: *status,
	}
	a.Stream.in <- event
}

func (a *adapter) FrameworkMessage(driver scheduler.SchedulerDriver, ei *mesos.ExecutorID, si *mesos.SlaveID, m string) {
	event := EventFrameworkMessage{
		EventMeta:  EventMeta{Driver: driver},
		ExecutorID: ei.GetValue(),
		SlaveID:    si.GetValue(),
		Message:    m,
	}
	a.Stream.in <- event
}

func (a *adapter) SlaveLost(driver scheduler.SchedulerDriver, si *mesos.SlaveID) {
	event := EventSlaveLost{
		EventMeta: EventMeta{Driver: driver},
		SlaveID:   si.GetValue(),
	}
	a.Stream.in <- event
}

func (a *adapter) ExecutorLost(driver scheduler.SchedulerDriver, ei *mesos.ExecutorID, si *mesos.SlaveID, st int) {
	event := EventExecutorLost{
		EventMeta:  EventMeta{Driver: driver},
		ExecutorID: ei.GetValue(),
		SlaveID:    si.GetValue(),
		Status:     st,
	}
	a.Stream.in <- event
}

func (a *adapter) Error(driver scheduler.SchedulerDriver, m string) {
	event := EventError{
		EventMeta: EventMeta{Driver: driver},
		Message:   m,
	}
	a.Stream.in <- event
	close(a.Stream.in) // driver has terminated, no more events
}
