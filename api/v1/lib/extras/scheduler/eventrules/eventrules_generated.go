package eventrules

// go generate
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"fmt"
	"sync"

	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

type (
	// Rule executes a filter, rule, or decorator function; if the returned event is nil then
	// no additional Rule func should be processed for the event.
	// Rule implementations should not modify the given event parameter (to avoid side effects).
	// If changes to the event object are needed, the suggested approach is to make a copy,
	// modify the copy, and pass the copy to the chain.
	Rule func(*scheduler.Event, error, Chain) (*scheduler.Event, error)

	// Chain is invoked by a Rule to continue processing an event. If the chain is not invoked,
	// no additional rules are processed.
	Chain func(*scheduler.Event, error) (*scheduler.Event, error)

	// Rules is a list of rules to be processed, in order.
	Rules []Rule

	// ErrorList accumulates errors that occur while processing a Chain of Rules. Accumulated
	// errors should be appended to the end of the list. An error list should never be empty.
	// Callers should use the package Error() func to properly accumulate (and flatten) errors.
	ErrorList []error
)

// chainIdentity is a Chain that returns the arguments as its results.
var chainIdentity = func(e *scheduler.Event, err error) (*scheduler.Event, error) {
	return e, err
}

// Eval is a Rule func that processes the set of all Rules. If there are no rules in the
// set then control is simply passed to the Chain.
func (rs Rules) Eval(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
	if len(rs) == 0 {
		return ch(e, err) // noop
	}
	// we know there's at least 1 rule in the initial list; start with it and let the chain
	// handle the iteration.
	return ch(rs[0](e, err, NewChain(rs)))
}

// Rule adapts Rules to the Rule interface, for convenient call chaining.
func (rs Rules) Rule() Rule { return rs.Eval }

// Error implements error; returns the message of the first error in the list.
func (es ErrorList) Error() string {
        switch len(es) {
        case 0:
                return "no errors"
        case 1:
                return es[0].Error()
        default:
                return fmt.Sprintf("%s (and %d more errors)", es[0], len(es)-1)
        }
}

// Error2 aggregates the given error params, returning nil if both are nil.
// Use Error2 to avoid the overhead of creating a slice when aggregating only 2 errors.
func Error2(a, b error) error {
	if a == nil {
		if b == nil {
			return nil
		}
		return b
	}
	if b == nil {
		return a
	}
	return Error(a, b)
}

// Err reduces an empty or singleton error list
func (es ErrorList) Err() error {
	if len(es) == 0 {
		return nil
	}
	if len(es) == 1 {
		return es[0]
	}
	return es
}

// IsErrorList returns true if err is a non-nil error list
func IsErrorList(err error) bool {
	if err != nil {
		_, ok := err.(ErrorList)
		return ok
	}
	return false
}

// Error aggregates, and then (shallowly) flattens, a list of errors accrued during rule processing.
// Returns nil if the given list of errors is empty or contains all nil errors.
func Error(es ...error) error {
	var result ErrorList
	for _, err := range es {
		if err != nil {
			if multi, ok := err.(ErrorList); ok {
				// flatten nested error lists
				if len(multi) > 0 {
					result = append(result, multi...)
				}
			} else {
				result = append(result, err)
			}
		}
	}
	return result.Err()
}

// TODO(jdef): other ideas for Rule decorators: If(bool), When(func() bool), Unless(bool)

// Once returns a Rule that executes the receiver only once.
func (r Rule) Once() Rule {
	var once sync.Once
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		once.Do(func() {
			e, err = r(e, err, ch)
		})
		return e, err
	}
}

// Poll invokes the receiving Rule if the chan is readable (may be closed), otherwise it drops the event.
// A nil chan will drop all events. May be useful, for example, when rate-limiting logged events.
func (r Rule) Poll(p <-chan struct{}) Rule {
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		select {
		case <-p:
			// do something
			return r(e, err, ch)
		default:
			// drop
			return ch(nil, err)
		}
	}
}

// EveryN invokes the receiving rule beginning with the first event seen and then every n'th
// time after that. If nthTime is less then 2 then this is a noop.
func (r Rule) EveryN(nthTime int) Rule {
	if nthTime < 2 {
		return r
	}
	var (
		i       = 1 // begin with the first event seen
		m       sync.Mutex
		forward = func() bool {
			m.Lock()
			i--
			if i == 0 {
				i = nthTime
				m.Unlock()
				return true
			}
			m.Unlock()
			return false
		}
	)
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		if forward() {
			return r(e, err, ch)
		}
		// else, drop
		return ch(nil, err)
	}
}

// DropOnError returns a Rule that generates a nil event if the error state != nil
func DropOnError() Rule {
	return Rule(nil).DropOnError()
}

// DropOnError decorates a rule by pre-checking the error state: if the error state != nil then
// the receiver is not invoked and (nil, err) is returned; otherwise control passes to the receiving
// rule.
func (r Rule) DropOnError() Rule {
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		if err != nil || e == nil {
			return e, err
		}
		if r != nil {
			return r(e, err, ch)
		}
		return ch(e, err)
	}
}

// NewChain returns a Chain that iterates through the given Rules, in order, stopping rule processing
// for any of the following cases:
//    - there are no more rules to process
//    - the event has been zero'ed out (nil)
// Any nil rules in the list are processed as skipped (noop's).
func NewChain(rs Rules) Chain {
	sz := len(rs)
	if sz == 0 {
		return chainIdentity
	}
	var (
		i     = 0
		chain Chain
	)
	chain = Chain(func(x *scheduler.Event, y error) (*scheduler.Event, error) {
		i++
		if i >= sz || x == nil {
			// we're at the end, or DROP was issued (x==nil)
			return x, y
		} else if rs[i] != nil {
			return rs[i](x, y, chain)
		} else {
			return chain(x, y)
		}

	})
	return chain
}

// AndThen returns a list of rules, beginning with the receiver, followed by DropOnError, and then
// all of the rules specified by the next parameter. The net effect is: execute the receiver rule
// and only if there is no error state, continue processing the next rules, in order.
func (r Rule) AndThen(next ...Rule) Rule {
	return append(Rules{r, DropOnError()}, next...).Eval
}

func DropOnSuccess() Rule {
	return Rule(nil).DropOnSuccess()
}

func (r Rule) DropOnSuccess() Rule {
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		if e != nil && err == nil {
			// bypass remainder of chain
			return e, err
		}
		if r != nil {
			return r(e, err, ch)
		}
		return ch(e, err)
	}
}

func (r Rule) OnFailure(next ...Rule) Rule {
	return append(Rules{r, DropOnSuccess()}, next...).Eval
}
