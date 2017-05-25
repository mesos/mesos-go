package calls

import (
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

// AckError wraps a caller-generated error and tracks the call that failed.
type AckError struct {
	Ack   *scheduler.Call
	Cause error
}

func (err *AckError) Error() string { return err.Cause.Error() }
