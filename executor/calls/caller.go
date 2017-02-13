package calls

import (
	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/executor"
)

// Caller is the public interface this framework executor's should consume
type (
	Caller interface {
		// Call issues a call to Mesos and properly manages call-specific HTTP response headers & data.
		Call(*executor.Call) (mesos.Response, error)
	}

	// CallerFunc is the functional adaptation of the Caller interface
	CallerFunc func(*executor.Call) (mesos.Response, error)

	// Decorator funcs usually return a Caller whose behavior has been somehow modified
	Decorator func(Caller) Caller

	// Decorators is a convenience type that applies multiple Decorator functions to a Caller
	Decorators []Decorator
)

// Call implements the Caller interface for CallerFunc
func (f CallerFunc) Call(c *executor.Call) (mesos.Response, error) { return f(c) }

// Apply is a convenient, nil-safe applicator that returns the result of d(c) iff d != nil; otherwise c
func (d Decorator) Apply(c Caller) (result Caller) {
	if d != nil {
		result = d(c)
	} else {
		result = c
	}
	return
}

// Apply is a convenience function that applies the combined product of the decorators to the given Caller.
func (ds Decorators) Apply(c Caller) Caller {
	return ds.Combine()(c)
}

// If returns the receiving Decorator if the given bool is true; otherwise returns a no-op
// Decorator instance.
func (d Decorator) If(b bool) Decorator {
	if d == nil {
		return noopDecorator
	}
	result := noopDecorator
	if b {
		result = d
	}
	return result
}

// Apply applies the Decorators in the order they're listed such that the last Decorator invoked
// generates the final (wrapping) Caller that is ultimately returned.
func (ds Decorators) Combine() (result Decorator) {
	actual := make(Decorators, 0, len(ds))
	for _, d := range ds {
		if d != nil {
			actual = append(actual, d)
		}
	}
	if len(actual) == 0 {
		result = noopDecorator
	} else {
		result = Decorator(func(h Caller) Caller {
			for _, d := range actual {
				h = d(h)
			}
			return h
		})
	}
	return
}

var noopDecorator = Decorator(func(h Caller) Caller { return h })

// CallNoData is a convenience func that executes the given Call using the provided Caller
// and always drops the response data.
func CallNoData(caller Caller, call *executor.Call) error {
	resp, err := caller.Call(call)
	if resp != nil {
		resp.Close()
	}
	return err
}
