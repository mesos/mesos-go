package auth

import (
	"errors"
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth/callback"
	"golang.org/x/net/context"
)

type Authenticatee interface {
	// Returns no errors if successfully authenticated, otherwise a single
	// error.
	Authenticate(ctx context.Context, handler callback.Handler) <-chan error
}

type AuthenticateeFunc func(ctx context.Context, handler callback.Handler) <-chan error

func (f AuthenticateeFunc) Authenticate(ctx context.Context, handler callback.Handler) <-chan error {
	return f(ctx, handler)
}

var (
	AuthenticationFailed = errors.New("authentication failed")

	authenticateeProviders = make(map[string]Authenticatee)
	providerLock           sync.Mutex
)

func RegisterAuthenticateeProvider(name string, auth Authenticatee) (err error) {
	providerLock.Lock()
	defer providerLock.Unlock()

	if _, found := authenticateeProviders[name]; found {
		err = fmt.Errorf("authentication provider already registered: %v", name)
	} else {
		authenticateeProviders[name] = auth
		log.V(1).Infof("registered authentication provider: %v", name)
	}
	return
}

func getAuthenticateeProvider(name string) (provider Authenticatee, ok bool) {
	providerLock.Lock()
	defer providerLock.Unlock()

	provider, ok = authenticateeProviders[name]
	return
}
