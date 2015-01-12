package callback

import (
	"fmt"
)

type Unsupported struct {
	Callback Interface
}

func (uc *Unsupported) Error() string {
	return fmt.Sprintf("Unsupported callback <%T>: %v", uc.Callback, uc.Callback)
}

type Interface interface {
	// marker interface
}

// may return an Unsupported error on failure
type Handler interface {
	Handle(callbacks ...Interface) error
}
