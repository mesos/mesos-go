package sasl

import (
	"golang.org/x/net/context"
)

// unexported to prevent collisions with context keys defined in
// other packages.
type _key int

// If this package defined other context keys, they would have
// different integer values.
const (
	statusKey _key = iota
)

func withStatus(ctx context.Context, s statusType) context.Context {
	return context.WithValue(ctx, statusKey, s)
}

func statusFrom(ctx context.Context) statusType {
	s, ok := ctx.Value(statusKey).(statusType)
	if !ok {
		panic("missing status in context")
	}
	return s
}
