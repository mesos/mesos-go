package mesos

import (
	"github.com/mesos/mesos-go/auth/sasl/callback"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type CredentialHandler struct {
	*mesos.Credential
}

func (h *CredentialHandler) Handle(callbacks ...callback.Interface) error {
	for _, cb := range callbacks {
		switch cb := cb.(type) {
		case *callback.Name:
			cb.Set(h.GetPrincipal())
		case *callback.Password:
			cb.Set(h.GetSecret())
		default:
			return &callback.Unsupported{cb}
		}
	}
	return nil
}
