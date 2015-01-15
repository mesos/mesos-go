package auth

import (
	"errors"
	"fmt"

	"github.com/mesos/mesos-go/auth/callback"
	"golang.org/x/net/context"
)

var (
	NoLoginNameErr = errors.New("missing login name in context")
)

func Login(ctx context.Context, handler callback.Handler) <-chan error {
	name, ok := LoginNameFrom(ctx)
	if !ok {
		return LoginError(NoLoginNameErr)
	}
	provider, ok := getAuthenticateeProvider(name)
	if !ok {
		return LoginError(fmt.Errorf("unidentified login name in context: %s", name))
	}
	return provider.Authenticate(ctx, handler)
}

func LoginError(err error) <-chan error {
	ch := make(chan error, 1)
	ch <- err
	close(ch)
	return ch
}

// unexported key type, avoids conflicts with other context-using packages
type loginKeyType int

const (
	loginNameKey loginKeyType = iota // name of login provider to use, e.g. SASL
)

func WithLoginName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, loginNameKey, name)
}

// return the value of the login name key from the context
func LoginNameFrom(ctx context.Context) (name string, ok bool) {
	name, ok = ctx.Value(loginNameKey).(string)
	return
}

// return the login name from the context, or emtpy string if none
func LoginName(ctx context.Context) string {
	name, _ := LoginNameFrom(ctx)
	return name
}
