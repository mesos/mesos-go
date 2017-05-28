package eventrules

// go generate
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"fmt"
	"sync"

	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

type (
	iface interface {
		// Eval executes a filter, rule, or decorator function; if the returned event is nil then
		// no additional rule evaluation should be processed for the event.
		// Eval implementations should not modify the given event parameter (to avoid side effects).
		// If changes to the event object are needed, the suggested approach is to make a copy,
		// modify the copy, and pass the copy to the chain.
		// Eval implementations SHOULD be safe to execute concurrently.
		Eval(*scheduler.Event, error, Chain) (*scheduler.Event, error)
	}

	// Rule is the functional adaptation of iface.
	// A nil Rule is valid: it is Eval'd as a noop.
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

var (
	_ = iface(Rule(nil))
	_ = iface(Rules{})

	// chainIdentity is a Chain that returns the arguments as its results.
	chainIdentity = func(e *scheduler.Event, err error) (*scheduler.Event, error) {
		return e, err
	}
)

// Eval is a convenience func that processes a nil Rule as a noop.
func (r Rule) Eval(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
	if r != nil {
		return r(e, err, ch)
	}
	return ch(e, err)
}

// Eval is a Rule func that processes the set of all Rules. If there are no rules in the
// set then control is simply passed to the Chain.
func (rs Rules) Eval(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
	return ch(rs.Chain()(e, err))
}

// Chain returns a Chain that evaluates the given Rules, in order, propagating the (*scheduler.Event, error)
// from Rule to Rule. Chain is safe to invoke concurrently.
func (rs Rules) Chain() Chain {
	if len(rs) == 0 {
		return chainIdentity
	}
	return func(e *scheduler.Event, err error) (*scheduler.Event, error) {
		return rs[0].Eval(e, err, rs[1:].Chain())
	}
}

// It is the semantic equivalent of Rules{r1, r2, ..., rn}.Rule() and exists purely for convenience.
func Concat(rs ...Rule) Rule { return Rules(rs).Eval }

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
		if list, ok := b.(ErrorList); ok {
			return flatten(list).Err()
		}
		return b
	}
	if b == nil {
		if list, ok := a.(ErrorList); ok {
			return flatten(list).Err()
		}
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

// Error aggregates, and then flattens, a list of errors accrued during rule processing.
// Returns nil if the given list of errors is empty or contains all nil errors.
func Error(es ...error) error {
	return flatten(es).Err()
}

func flatten(errors []error) ErrorList {
	if errors == nil || len(errors) == 0 {
		return nil
	}
	result := make([]error, 0, len(errors))
	for _, err := range errors {
		if err != nil {
			if multi, ok := err.(ErrorList); ok {
				result = append(result, flatten(multi)...)
			} else {
				result = append(result, err)
			}
		}
	}
	return ErrorList(result)
}

// TODO(jdef): other ideas for Rule decorators: When(func() bool), WhenNot(func() bool), OrElse(...Rule)

// If only executes the receiving rule if b is true; otherwise, the returned rule is a noop.
func (r Rule) If(b bool) Rule {
	if b {
		return r
	}
	return nil
}

// Unless only executes the receiving rule if b is false; otherwise, the returned rule is a noop.
func (r Rule) Unless(b bool) Rule {
	if !b {
		return r
	}
	return nil
}

// Once returns a Rule that executes the receiver only once.
func (r Rule) Once() Rule {
	if r == nil {
		return nil
	}
	var once sync.Once
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		ruleInvoked := false
		once.Do(func() {
			e, err = r(e, err, ch)
			ruleInvoked = true
		})
		if !ruleInvoked {
			e, err = ch(e, err)
		}
		return e, err
	}
}

// Poll invokes the receiving Rule if the chan is readable (may be closed), otherwise it skips the rule.
// A nil chan will always skip the rule. May be useful, for example, when rate-limiting logged events.
func (r Rule) Poll(p <-chan struct{}) Rule {
	if p == nil || r == nil {
		return nil
	}
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		select {
		case <-p:
			// do something
			// TODO(jdef): optimization: if we detect the chan is closed, affect a state change
			// whereby this select is no longer invoked (and always pass control to r).
			return r(e, err, ch)
		default:
			return ch(e, err)
		}
	}
}

// EveryN invokes the receiving rule beginning with the first event seen and then every n'th
// time after that. If nthTime is less then 2 then this call is a noop (the receiver is returned).
func (r Rule) EveryN(nthTime int) Rule {
	if nthTime < 2 || r == nil {
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
		return ch(e, err)
	}
}

// Drop aborts the Chain and returns the (*scheduler.Event, error) tuple as-is.
func Drop() Rule {
	return Rule(nil).ThenDrop()
}

// ThenDrop executes the receiving rule, but aborts the Chain, and returns the (*scheduler.Event, error) tuple as-is.
func (r Rule) ThenDrop() Rule {
	return func(e *scheduler.Event, err error, _ Chain) (*scheduler.Event, error) {
		return r.Eval(e, err, chainIdentity)
	}
}

// DropOnError returns a Rule that generates a nil event if the error state != nil
func DropOnError() Rule {
	return Rule(nil).DropOnError()
}

// DropOnError decorates a rule by pre-checking the error state: if the error state != nil then
// the receiver is not invoked and (e, err) is returned; otherwise control passes to the receiving rule.
func (r Rule) DropOnError() Rule {
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		if err != nil {
			return e, err
		}
		return r.Eval(e, err, ch)
	}
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
		if err == nil {
			// bypass remainder of chain
			return e, err
		}
		return r.Eval(e, err, ch)
	}
}

func (r Rule) OnFailure(next ...Rule) Rule {
	return append(Rules{r, DropOnSuccess()}, next...).Eval
}
