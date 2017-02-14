package calls

import (
	"errors"
	"math/rand"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

// Filters sets a scheduler.Call's internal Filters, required for Accept and Decline calls.
func Filters(fo ...mesos.FilterOpt) scheduler.CallOpt {
	return func(c *scheduler.Call) {
		switch *c.Type {
		case scheduler.Call_ACCEPT:
			c.Accept.Filters = mesos.OptionalFilters(fo...)
		case scheduler.Call_ACCEPT_INVERSE_OFFERS:
			c.AcceptInverseOffers.Filters = mesos.OptionalFilters(fo...)
		case scheduler.Call_DECLINE:
			c.Decline.Filters = mesos.OptionalFilters(fo...)
		case scheduler.Call_DECLINE_INVERSE_OFFERS:
			c.DeclineInverseOffers.Filters = mesos.OptionalFilters(fo...)
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
func Subscribe(info *mesos.FrameworkInfo) *scheduler.Call {
	return &scheduler.Call{
		Type:        scheduler.Call_SUBSCRIBE.Enum(),
		FrameworkID: info.GetID(),
		Subscribe:   &scheduler.Call_Subscribe{FrameworkInfo: info},
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

// AcceptInverseOffers returns an accept-inverse-offers call for the given offer IDs.
// Callers are expected to fill in the FrameworkID and Filters.
func AcceptInverseOffers(offerIDs ...mesos.OfferID) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_ACCEPT_INVERSE_OFFERS.Enum(),
		AcceptInverseOffers: &scheduler.Call_AcceptInverseOffers{
			InverseOfferIDs: offerIDs,
		},
	}
}

// DeclineInverseOffers returns a decline-inverse-offers call for the given offer IDs.
// Callers are expected to fill in the FrameworkID and Filters.
func DeclineInverseOffers(offerIDs ...mesos.OfferID) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_DECLINE_INVERSE_OFFERS.Enum(),
		DeclineInverseOffers: &scheduler.Call_DeclineInverseOffers{
			InverseOfferIDs: offerIDs,
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

// Suppress returns a suppress call.
// Callers are expected to fill in the FrameworkID.
func Suppress() *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_SUPPRESS.Enum(),
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
func Kill(taskID, agentID string) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_KILL.Enum(),
		Kill: &scheduler.Call_Kill{
			TaskID:  mesos.TaskID{Value: taskID},
			AgentID: optionalAgentID(agentID),
		},
	}
}

// Shutdown returns a shutdown call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func Shutdown(executorID, agentID string) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_SHUTDOWN.Enum(),
		Shutdown: &scheduler.Call_Shutdown{
			ExecutorID: mesos.ExecutorID{Value: executorID},
			AgentID:    mesos.AgentID{Value: agentID},
		},
	}
}

// Acknowledge returns an acknowledge call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func Acknowledge(agentID, taskID string, uuid []byte) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_ACKNOWLEDGE.Enum(),
		Acknowledge: &scheduler.Call_Acknowledge{
			AgentID: mesos.AgentID{Value: agentID},
			TaskID:  mesos.TaskID{Value: taskID},
			UUID:    uuid,
		},
	}
}

// ReconcileTasks constructs a []Call_Reconcile_Task from the given mappings:
//     map[string]string{taskID:agentID}
// Map keys (taskID's) are required to be non-empty, but values (agentID's) *may* be empty.
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
			result[i].AgentID = optionalAgentID(v)
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
func Message(agentID, executorID string, data []byte) *scheduler.Call {
	return &scheduler.Call{
		Type: scheduler.Call_MESSAGE.Enum(),
		Message: &scheduler.Call_Message{
			AgentID:    mesos.AgentID{Value: agentID},
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

func optionalAgentID(agentID string) *mesos.AgentID {
	if agentID == "" {
		return nil
	}
	return &mesos.AgentID{Value: agentID}
}

func errInvalidCall(reason string) error {
	return errors.New("invalid call: " + reason)
}
