package calls

import (
	"errors"
	"math/rand"
	"time"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/scheduler"
)

// Filters sets a scheduler.Call's internal Filters, required for Accept and Decline calls.
func Filters(fo ...mesos.FilterOpt) scheduler.CallOpt {
	return func(c *scheduler.Call) {
		switch *c.Type {
		case scheduler.Call_ACCEPT:
			c.Accept.Filters = mesos.OptionalFilters(fo...)
		case scheduler.Call_DECLINE:
			c.Decline.Filters = mesos.OptionalFilters(fo...)
		default:
			panic("filters not supported for type " + c.Type.String())
		}
	}
}

// RefuseSecondsWithJitter returns a calls.Filters option that sets RefuseSeconds to a random number
// of seconds between 0 and the given duration.
func RefuseSecondsWithJitter(r *rand.Rand, d time.Duration) scheduler.CallOpt {
	return Filters(func(f *mesos.Filters) {
		s := time.Duration(r.Int63n(int64(d))).Seconds()
		f.RefuseSeconds = &s
	})
}

// Framework sets a scheduler.Call's FrameworkID
func Framework(id string) scheduler.CallOpt {
	return func(c *scheduler.Call) {
		c.FrameworkID = &mesos.FrameworkID{Value: id}
	}
}

// Subscribe returns a subscribe call with the given parameters.
// The call's FrameworkID is automatically filled in from the info specification.
func Subscribe(force bool, info *mesos.FrameworkInfo) *scheduler.Call {
	return &scheduler.Call{
		Type:        scheduler.Call_SUBSCRIBE.Enum(),
		FrameworkID: info.GetID(),
		Subscribe:   &scheduler.Call_Subscribe{FrameworkInfo: info, Force: force},
	}
}

type acceptBuilder struct {
	offerIDs   map[mesos.OfferID]struct{}
	operations []mesos.Offer_Operation
}

type AcceptOpt func(*acceptBuilder)

type OfferOperations []mesos.Offer_Operation

// WithOffers allows a client to pair some set of OfferOperations with multiple resource offers.
// Example: calls.Accept(calls.OfferOperations{calls.OpLaunch(tasks...)}.WithOffers(offers...))
func (ob OfferOperations) WithOffers(ids ...mesos.OfferID) AcceptOpt {
	return func(ab *acceptBuilder) {
		for i := range ids {
			ab.offerIDs[ids[i]] = struct{}{}
		}
		ab.operations = append(ab.operations, ob...)
	}
}

// Accept returns an accept call with the given parameters.
// Callers are expected to fill in the FrameworkID and Filters.
func Accept(ops ...AcceptOpt) *scheduler.Call {
	ab := &acceptBuilder{
		offerIDs: make(map[mesos.OfferID]struct{}, len(ops)),
	}
	for _, op := range ops {
		op(ab)
	}
	offerIDs := make([]mesos.OfferID, 0, len(ab.offerIDs))
	for id := range ab.offerIDs {
		offerIDs = append(offerIDs, id)
	}
	return &scheduler.Call{
		Type: scheduler.Call_ACCEPT.Enum(),
		Accept: &scheduler.Call_Accept{
			OfferIDs:   offerIDs,
			Operations: ab.operations,
		},
	}
}

// OpLaunch returns a launch operation builder for the given tasks
func OpLaunch(ti ...mesos.TaskInfo) mesos.Offer_Operation {
	return mesos.Offer_Operation{
		Type: mesos.LAUNCH.Enum(),
		Launch: &mesos.Offer_Operation_Launch{
			TaskInfos: ti,
		},
	}
}

func OpReserve(rs ...mesos.Resource) mesos.Offer_Operation {
	return mesos.Offer_Operation{
		Type: mesos.RESERVE.Enum(),
		Reserve: &mesos.Offer_Operation_Reserve{
			Resources: rs,
		},
	}
}

func OpUnreserve(rs ...mesos.Resource) mesos.Offer_Operation {
	return mesos.Offer_Operation{
		Type: mesos.UNRESERVE.Enum(),
		Unreserve: &mesos.Offer_Operation_Unreserve{
			Resources: rs,
		},
	}
}

func OpCreate(rs ...mesos.Resource) mesos.Offer_Operation {
	return mesos.Offer_Operation{
		Type: mesos.CREATE.Enum(),
		Create: &mesos.Offer_Operation_Create{
			Volumes: rs,
		},
	}
}

func OpDestroy(rs ...mesos.Resource) mesos.Offer_Operation {
	return mesos.Offer_Operation{
		Type: mesos.DESTROY.Enum(),
		Destroy: &mesos.Offer_Operation_Destroy{
			Volumes: rs,
		},
	}
}

// Revive returns a revive call.
// Callers are expected to fill in the FrameworkID.
func Revive() *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_REVIVE.Enum(),
	}
}

// Decline returns a decline call with the given parameters.
// Callers are expected to fill in the FrameworkID and Filters.
func Decline(offerIDs ...mesos.OfferID) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_DECLINE.Enum(),
		Decline: &scheduler.Call_Decline{
			OfferIDs: offerIDs,
		},
	}
}

// Kill returns a kill call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func Kill(taskID, slaveID string) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_KILL.Enum(),
		Kill: &scheduler.Call_Kill{
			TaskID:  mesos.TaskID{Value: taskID},
			SlaveID: optionalSlaveID(slaveID),
		},
	}
}

// Shutdown returns a shutdown call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func Shutdown(executorID, slaveID string) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_SHUTDOWN.Enum(),
		Shutdown: &scheduler.Call_Shutdown{
			ExecutorID: mesos.ExecutorID{Value: executorID},
			SlaveID:    mesos.SlaveID{Value: slaveID},
		},
	}
}

// Acknowledge returns an acknowledge call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func Acknowledge(slaveID, taskID string, uuid []byte) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_ACKNOWLEDGE.Enum(),
		Acknowledge: &scheduler.Call_Acknowledge{
			SlaveID: mesos.SlaveID{Value: slaveID},
			TaskID:  mesos.TaskID{Value: taskID},
			UUID:    uuid,
		},
	}
}

// ReconcileTasks constructs a []Call_Reconcile_Task from the given mappings:
//     map[string]string{taskID:slaveID}
// Map keys (taskID's) are required to be non-empty, but values (slaveID's) *may* be empty.
func ReconcileTasks(tasks map[string]string) scheduler.ReconcileOpt {
	return func(cr *scheduler.Call_Reconcile) {
		if len(tasks) == 0 {
			cr.Tasks = nil
			return
		}
		result := make([]scheduler.Call_Reconcile_Task, len(tasks))
		i := 0
		for k, v := range tasks {
			result[i].TaskID = mesos.TaskID{Value: k}
			result[i].SlaveID = optionalSlaveID(v)
			i++
		}
		cr.Tasks = result
	}
}

// Reconcile returns a reconcile call with the given parameters.
// See ReconcileTask.
// Callers are expected to fill in the FrameworkID.
func Reconcile(opts ...scheduler.ReconcileOpt) *scheduler.Call {
	return &scheduler.Call{
		Type:      scheduler.Call_RECONCILE.Enum(),
		Reconcile: (&scheduler.Call_Reconcile{}).With(opts...),
	}
}

// Message returns a message call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func Message(slaveID, executorID string, data []byte) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_MESSAGE.Enum(),
		Message: &scheduler.Call_Message{
			SlaveID:    mesos.SlaveID{Value: slaveID},
			ExecutorID: mesos.ExecutorID{Value: executorID},
			Data:       data,
		},
	}
}

// Request returns a resource request call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func Request(requests ...mesos.Request) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_REQUEST.Enum(),
		Request: &scheduler.Call_Request{
			Requests: requests,
		},
	}
}

func optionalSlaveID(slaveID string) *mesos.SlaveID {
	if slaveID == "" {
		return nil
	}
	return &mesos.SlaveID{Value: slaveID}
}

func errInvalidCall(reason string) error {
	return errors.New("invalid call: " + reason)
}
