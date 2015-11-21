package scheduler

import (
	"errors"

	"github.com/mesos/mesos-go"
)

// A CallOpt is a functional option type for Calls.
type CallOpt func(*Call)

// With applies the given CallOpts to the receiving Call, returning it.
func (c *Call) With(opts ...CallOpt) *Call {
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Framework sets a call's FrameworkID
func Framework(id string) CallOpt {
	return func(c *Call) {
		c.FrameworkID = &mesos.FrameworkID{Value: id}
	}
}

// A ReconcileOpt is a functional option type for Call_Reconcile
type ReconcileOpt func(*Call_Reconcile)

// With applies the given ReconcileOpt's to the receiving Call_Reconcile, returning it.
func (r *Call_Reconcile) With(opts ...ReconcileOpt) *Call_Reconcile {
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// SubscribeCall returns a subscribe call with the given parameters.
// The call's FrameworkID is automatically filled in from the info specification.
func SubscribeCall(force bool, info *mesos.FrameworkInfo) *Call {
	return &Call{
		Type:        Call_SUBSCRIBE.Enum(),
		FrameworkID: info.GetID(),
		Subscribe:   &Call_Subscribe{FrameworkInfo: info, Force: force},
	}
}

// AcceptCall returns an accept call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func AcceptCall(offerIDs []mesos.OfferID, ops []mesos.Offer_Operation, fo ...mesos.FilterOpt) *Call {
	return &Call{
		Type: Call_ACCEPT.Enum(),
		Accept: &Call_Accept{
			OfferIDs:   offerIDs,
			Operations: ops,
			Filters:    mesos.OptionalFilters(fo...),
		},
	}
}

// DeclineCall returns a decline call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func DeclineCall(offerIDs []mesos.OfferID, fo ...mesos.FilterOpt) *Call {
	return &Call{
		Type: Call_DECLINE.Enum(),
		Decline: &Call_Decline{
			OfferIDs: offerIDs,
			Filters:  mesos.OptionalFilters(fo...),
		},
	}
}

// KillCall returns a kill call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func KillCall(taskID, agentID string) *Call {
	return &Call{
		Type: Call_KILL.Enum(),
		Kill: &Call_Kill{
			TaskID:  mesos.TaskID{Value: taskID},
			AgentID: optionalAgentID(agentID),
		},
	}
}

// ShutdownCall returns a shutdown call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func ShutdownCall(executorID, agentID string) *Call {
	return &Call{
		Type: Call_SHUTDOWN.Enum(),
		Shutdown: &Call_Shutdown{
			ExecutorID: mesos.ExecutorID{Value: executorID},
			AgentID:    mesos.AgentID{Value: agentID},
		},
	}
}

// AcknowledgeCall returns an acknowledge call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func AcknowledgeCall(agentID, taskID string, uuid []byte) *Call {
	return &Call{
		Type: Call_ACKNOWLEDGE.Enum(),
		Acknowledge: &Call_Acknowledge{
			AgentID: mesos.AgentID{Value: agentID},
			TaskID:  mesos.TaskID{Value: taskID},
			UUID:    uuid,
		},
	}
}

// ReconcileTasks constructs a []Call_Reconcile_Task from the given mappings:
//     map[string]string{taskID:agentID}
// Map keys (taskID's) are required to be non-empty, but values (agentID's) *may* be empty.
func ReconcileTasks(tasks map[string]string) ReconcileOpt {
	return func(cr *Call_Reconcile) {
		if len(tasks) == 0 {
			cr.Tasks = nil
			return
		}
		result := make([]Call_Reconcile_Task, len(tasks))
		i := 0
		for k, v := range tasks {
			result[i].TaskID = mesos.TaskID{Value: k}
			result[i].AgentID = optionalAgentID(v)
			i++
		}
		cr.Tasks = result
	}
}

// ReconcileCall returns a reconcile call with the given parameters.
// See ReconcileTask.
// Callers are expected to fill in the FrameworkID.
func ReconcileCall(opts ...ReconcileOpt) *Call {
	return &Call{
		Type:      Call_RECONCILE.Enum(),
		Reconcile: (&Call_Reconcile{}).With(opts...),
	}
}

// MessageCall returns a message call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func MessageCall(agentID, executorID string, data []byte) *Call {
	return &Call{
		Type: Call_MESSAGE.Enum(),
		Message: &Call_Message{
			AgentID:    mesos.AgentID{Value: agentID},
			ExecutorID: mesos.ExecutorID{Value: executorID},
			Data:       data,
		},
	}
}

// RequestCall returns a resource request call with the given parameters.
// Callers are expected to fill in the FrameworkID.
func RequestCall(requests ...mesos.Request) *Call {
	return &Call{
		Type: Call_REQUEST.Enum(),
		Request: &Call_Request{
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
