package mech

import (
	"github.com/mesos/mesos-go/auth/sasl/callback"
)

type Interface interface {
	Handler() callback.Handler
}

// return a mechanism and it's initialization step
type Factory func(h callback.Handler) (Interface, StepFunc, error)

type StepFunc func(m Interface, data []byte) (StepFunc, []byte, error)
