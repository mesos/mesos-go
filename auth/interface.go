package auth

import (
	"errors"

	"github.com/mesos/mesos-go/upid"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type Authenticatee interface {
	// 'pid' is the process to authenticate against (master).
	// 'client' is the process to be authenticated (slave / framework).
	// 'credential' is used to authenticate the 'client'.
	// Returns true if successfully authenticated otherwise false or an
	// error. Note that we distinguish authentication failure (false)
	// from a failed future in the event the future failed due to a
	// transient error and authentication can (should) be
	// retried. Discarding the future will cause the future to fail if
	// it hasn't already completed since we have already started the
	// authentication procedure and can't reliably cancel.
	Authenticate(pid, client upid.UPID, credendial mesos.Credential) <-chan err
}

var (
	AuthenticationFailed = errors.New("authentication failed")
)
