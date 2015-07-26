package streaming

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/scheduler"
)

//TODO(jdef) better encapsulate the protobuf API's; don't expose them to callers of this package

type EventMeta struct {
	Driver scheduler.SchedulerDriver
}

type EventRegistered struct {
	EventMeta
	FrameworkID string
	MasterInfo  *mesos.MasterInfo
}

type EventReregistered struct {
	EventMeta
	MasterInfo *mesos.MasterInfo
}

type EventDisconnected struct {
	EventMeta
}

type EventResourceOffers struct {
	EventMeta
	Offers []*mesos.Offer
}

type EventOfferRescinded struct {
	EventMeta
	OfferID string
}

type EventStatusUpdate struct {
	EventMeta
	TaskStatus mesos.TaskStatus
}

type EventFrameworkMessage struct {
	EventMeta
	ExecutorID string
	SlaveID    string
	Message    string
}

type EventSlaveLost struct {
	EventMeta
	SlaveID string
}

type EventExecutorLost struct {
	EventMeta
	ExecutorID string
	SlaveID    string
	Status     int
}

type EventError struct {
	EventMeta
	Message string
}
