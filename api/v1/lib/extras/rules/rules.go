// +build ignore

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"text/template"
)

type (
	config struct {
		Package        string
		Imports        []string
		EventType      string
		EventPrototype string
	}
)

func (c *config) String() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("%#v", ([]string)(c.Imports))
}

func (c *config) Set(s string) error {
	c.Imports = append(c.Imports, s)
	return nil
}

func main() {
	var (
		c = config{
			Package:   os.Getenv("GOPACKAGE"),
			EventType: "Event",
		}
		defaultOutput = "foo.go"
	)
	if c.Package != "" {
		defaultOutput = c.Package + "_generated.go"
	}

	output := defaultOutput

	flag.StringVar(&c.Package, "package", c.Package, "destination package")
	flag.StringVar(&c.EventType, "event_type", c.EventType, "golang type of the event to be processed")
	flag.StringVar(&output, "output", output, "path of the to-be-generated file")
	flag.Var(&c, "import", "packages to import")
	flag.Parse()

	if c.Package == "" {
		c.Package = "foo"
	}
	if c.EventType == "" {
		c.EventType = "Event"
		c.EventPrototype = "Event{}"
	} else if strings.HasPrefix(c.EventType, "*") {
		// TODO(jdef) don't assume that event type is a struct or *struct
		c.EventPrototype = "&" + c.EventType[1:] + "{}"
	} else {
		c.EventPrototype = c.EventType[1:] + "{}"
	}
	if output == "" {
		output = defaultOutput
	}

	testOutput := output + "_test"
	if strings.HasSuffix(output, ".go") {
		testOutput = output[:len(output)-3] + "_test.go"
	}

	// main template
	f, err := os.Create(output)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	rulesTemplate.Execute(f, c)

	// unit test template
	f, err = os.Create(testOutput)
	if err != nil {
		log.Fatal(err)
	}
	testTemplate.Execute(f, c)
}

var rulesTemplate = template.Must(template.New("").Parse(`package {{.Package}}

// go generate
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"fmt"
	"sync"
{{range .Imports}}
	{{ printf "%q" . }}
{{ end -}}
)

type (
	iface interface {
		// Eval executes a filter, rule, or decorator function; if the returned event is nil then
		// no additional rule evaluation should be processed for the event.
		// Eval implementations should not modify the given event parameter (to avoid side effects).
		// If changes to the event object are needed, the suggested approach is to make a copy,
		// modify the copy, and pass the copy to the chain.
		// Eval implementations SHOULD be safe to execute concurrently.
		Eval({{.EventType}}, error, Chain) ({{.EventType}}, error)
	}

	// Rule is the functional adaptation of iface.
	// A nil Rule is valid: it is Eval'd as a noop.
	Rule func({{.EventType}}, error, Chain) ({{.EventType}}, error)

	// Chain is invoked by a Rule to continue processing an event. If the chain is not invoked,
	// no additional rules are processed.
	Chain func({{.EventType}}, error) ({{.EventType}}, error)

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
	chainIdentity = func(e {{.EventType}}, err error) ({{.EventType}}, error) {
		return e, err
	}
)

// Eval is a convenience func that processes a nil Rule as a noop.
func (r Rule) Eval(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
	if r != nil {
		return r(e, err, ch)
	}
	return ch(e, err)
}

// Eval is a Rule func that processes the set of all Rules. If there are no rules in the
// set then control is simply passed to the Chain.
func (rs Rules) Eval(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
	return ch(rs.Chain()(e, err))
}

// Chain returns a Chain that evaluates the given Rules, in order, propagating the ({{.EventType}}, error)
// from Rule to Rule. Chain is safe to invoke concurrently.
func (rs Rules) Chain() Chain {
	if len(rs) == 0 {
		return chainIdentity
	}
	return func(e {{.EventType}}, err error) ({{.EventType}}, error) {
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
	return func(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
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
	return func(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
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
	return func(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
		if forward() {
			return r(e, err, ch)
		}
		return ch(e, err)
	}
}

// Drop aborts the Chain and returns the ({{.EventType}}, error) tuple as-is.
func Drop() Rule {
	return Rule(nil).ThenDrop()
}

// ThenDrop executes the receiving rule, but aborts the Chain, and returns the ({{.EventType}}, error) tuple as-is.
func (r Rule) ThenDrop() Rule {
	return func(e {{.EventType}}, err error, _ Chain) ({{.EventType}}, error) {
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
	return func(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
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
	return func(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
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
`))

var testTemplate = template.Must(template.New("").Parse(`package {{.Package}}

// go generate
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"errors"
	"reflect"
	"testing"
{{range .Imports}}
	{{ printf "%q" . }}
{{ end -}}
)

func prototype() {{.EventType}} { return {{.EventPrototype}} }

func counter(i *int) Rule {
	return func(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
		*i++
		return ch(e, err)
	}
}

func tracer(r Rule, name string, t *testing.T) Rule {
	return func(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
		t.Log("executing", name)
		return r(e, err, ch)
	}
}

func returnError(re error) Rule {
	return func(e {{.EventType}}, err error, ch Chain) ({{.EventType}}, error) {
		return ch(e, Error2(err, re))
	}
}

func chainCounter(i *int, ch Chain) Chain {
	return func(e {{.EventType}}, err error) ({{.EventType}}, error) {
		*i++
		return ch(e, err)
	}
}

func TestChainIdentity(t *testing.T) {
	var i int
	counterRule := counter(&i)

	e, err := Rules{counterRule}.Eval(nil, nil, chainIdentity)
	if e != nil {
		t.Error("expected nil event instead of", e)
	}
	if err != nil {
		t.Error("expected nil error instead of", err)
	}
	if i != 1 {
		t.Error("expected 1 rule execution instead of", i)
	}
}

func TestRules(t *testing.T) {
	var (
		p = prototype()
		a = errors.New("a")
	)

	// multiple rules in Rules should execute, dropping nil rules along the way
	for _, tc := range []struct {
		e   {{.EventType}}
		err error
	}{
		{nil, nil},
		{nil, a},
		{p, nil},
		{p, a},
	} {
		var (
			i    int
			rule = Concat(
				nil,
				tracer(counter(&i), "counter1", t),
				nil,
				tracer(counter(&i), "counter2", t),
				nil,
			)
			e, err = rule(tc.e, tc.err, chainIdentity)
		)
		if e != tc.e {
			t.Errorf("expected prototype event %q instead of %q", tc.e, e)
		}
		if err != tc.err {
			t.Errorf("expected %q error instead of %q", tc.err, err)
		}
		if i != 2 {
			t.Error("expected 2 rule executions instead of", i)
		}

		// empty Rules should not change event, err
		e, err = Rules{}.Eval(tc.e, tc.err, chainIdentity)
		if e != tc.e {
			t.Errorf("expected prototype event %q instead of %q", tc.e, e)
		}
		if err != tc.err {
			t.Errorf("expected %q error instead of %q", tc.err, err)
		}
	}
}

func TestError2(t *testing.T) {
	var (
		a = errors.New("a")
		b = errors.New("b")
	)
	for i, tc := range []struct {
		a            error
		b            error
		wants        error
		wantsMessage string
	}{
		{nil, nil, nil, ""},
		{nil, ErrorList{nil}, nil, ""},
		{ErrorList{nil}, ErrorList{nil}, nil, ""},
		{ErrorList{ErrorList{nil}}, ErrorList{nil}, nil, ""},
		{a, nil, a, "a"},
		{ErrorList{a}, nil, a, "a"},
		{ErrorList{nil, a, ErrorList{}}, nil, a, "a"},
		{nil, b, b, "b"},
		{nil, ErrorList{b}, b, "b"},
		{a, b, ErrorList{a, b}, "a (and 1 more errors)"},
		{a, ErrorList{b}, ErrorList{a, b}, "a (and 1 more errors)"},
		{a, ErrorList{nil, ErrorList{b, ErrorList{}, nil}}, ErrorList{a, b}, "a (and 1 more errors)"},
	} {
		var (
			sameError bool
			result    = Error2(tc.a, tc.b)
		)
		// jump through hoops because we can't directly compare two errors with == if
		// they're both ErrorList.
		if IsErrorList(result) == IsErrorList(tc.wants) { // both are lists or neither
			sameError = (!IsErrorList(result) && result == tc.wants) ||
				(IsErrorList(result) && reflect.DeepEqual(result, tc.wants))
		}
		if !sameError {
			t.Fatalf("test case %d failed, expected %v instead of %v", i, tc.wants, result)
		}
		if result != nil && tc.wantsMessage != result.Error() {
			t.Fatalf("test case %d failed, expected message %q instead of %q",
				i, tc.wantsMessage, result.Error())
		}
	}
}

func TestAndThen(t *testing.T) {
	var (
		i, j int
		p    = prototype()
		r1   = counter(&i)
		r2   = Rule(nil).AndThen(counter(&i))
		a    = errors.New("a")
	)
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for k, r := range []Rule{r1, r2} {
		e, err := r(p, a, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if err != a {
			t.Error("unexpected error", err)
		}
		if i != 1 {
			t.Errorf("expected count of 1 instead of %d", i)
		}
		if j != (k + 1) {
			t.Errorf("expected chain count of %d instead of %d", (k + 1), j)
		}
	}
}

func TestDropOnError(t *testing.T) {
	var (
		i, j int
		p    = prototype()
		r1   = counter(&i)
		r2   = counter(&i).DropOnError()
		a    = errors.New("a")
	)
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for _, r := range []Rule{r1, r2} {
		e, err := r(p, a, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if err != a {
			t.Error("unexpected error", err)
		}
		if i != 1 {
			t.Errorf("expected count of 1 instead of %d", i)
		}
		if j != 1 {
			t.Errorf("expected chain count of 1 instead of %d", j)
		}
	}
}

func TestDropOnSuccess(t *testing.T) {
	var (
		i, j int
		p    = prototype()
		r1   = counter(&i)
		r2   = counter(&i).DropOnSuccess()
	)
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for _, r := range []Rule{r1, r2} {
		e, err := r(p, nil, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if err != nil {
			t.Error("unexpected error", err)
		}
		if i != 1 {
			t.Errorf("expected count of 1 instead of %d", i)
		}
		if j != 1 {
			t.Errorf("expected chain count of 1 instead of %d", j)
		}
	}
}

func TestThenDrop(t *testing.T) {
	for _, anErr := range []error{nil, errors.New("a")} {
		var (
			i, j int
			p    = prototype()
			r1   = counter(&i)
			r2   = counter(&i).ThenDrop()
		)
		// r1 and r2 should execute the counter rule
		for k, r := range []Rule{r1, r2} {
			e, err := r(p, anErr, chainCounter(&j, chainIdentity))
			if e != p {
				t.Errorf("expected event %q instead of %q", p, e)
			}
			if err != anErr {
				t.Errorf("expected %v instead of error %v", anErr, err)
			}
			if i != (k + 1) {
				t.Errorf("expected count of %d instead of %d", (k + 1), i)
			}
			if j != 1 {
				t.Errorf("expected chain count of 1 instead of %d", j)
			}
		}
	}
}

func TestDrop(t *testing.T) {
	for _, anErr := range []error{nil, errors.New("a")} {
		var (
			i, j int
			p    = prototype()
			r1   = counter(&i)
			r2   = Rules{Drop(), counter(&i)}.Eval
		)
		// r1 should execute the counter rule
		// r2 should NOT exexute the counter rule
		for k, r := range []Rule{r1, r2} {
			e, err := r(p, anErr, chainCounter(&j, chainIdentity))
			if e != p {
				t.Errorf("expected event %q instead of %q", p, e)
			}
			if err != anErr {
				t.Errorf("expected %v instead of error %v", anErr, err)
			}
			if i != 1 {
				t.Errorf("expected count of 1 instead of %d", i)
			}
			if j != (k + 1) {
				t.Errorf("expected chain count of %d instead of %d with error %v", (k + 1), j, anErr)
			}
		}
	}
}

func TestIf(t *testing.T) {
	var (
		i, j int
		p    = prototype()
		r1   = counter(&i).If(true).Eval
		r2   = counter(&i).If(false).Eval
	)
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for k, r := range []Rule{r1, r2} {
		e, err := r(p, nil, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if err != nil {
			t.Error("unexpected error", err)
		}
		if i != 1 {
			t.Errorf("expected count of 1 instead of %d", i)
		}
		if j != (k + 1) {
			t.Errorf("expected chain count of %d instead of %d", (k + 1), j)
		}
	}
}

func TestUnless(t *testing.T) {
	var (
		i, j int
		p    = prototype()
		r1   = counter(&i).Unless(false).Eval
		r2   = counter(&i).Unless(true).Eval
	)
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for k, r := range []Rule{r1, r2} {
		e, err := r(p, nil, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if err != nil {
			t.Error("unexpected error", err)
		}
		if i != 1 {
			t.Errorf("expected count of 1 instead of %d", i)
		}
		if j != (k + 1) {
			t.Errorf("expected chain count of %d instead of %d", (k + 1), j)
		}
	}
}

func TestOnce(t *testing.T) {
	var (
		i, j int
		p    = prototype()
		r1   = counter(&i).Once().Eval
		r2   = Rule(nil).Once().Eval
	)
	for k, r := range []Rule{r1, r2} {
		for x := 0; x < 5; x++ {
			e, err := r(p, nil, chainCounter(&j, chainIdentity))
			if e != p {
				t.Errorf("expected event %q instead of %q", p, e)
			}
			if err != nil {
				t.Error("unexpected error", err)
			}
			if i != 1 {
				t.Errorf("expected count of 1 instead of %d", i)
			}
			if y := (k * 5) + x + 1; j != y {
				t.Errorf("expected chain count of %d instead of %d", y, j)
			}
		}
	}
}

func TestPoll(t *testing.T) {
	var (
		ch1 <-chan struct{}          // always nil
		ch2 = make(chan struct{})    // non-nil, blocking
		ch3 = make(chan struct{}, 1) // non-nil, non-blocking then blocking
		ch4 = make(chan struct{})    // non-nil, closed
	)
	ch3 <- struct{}{}
	close(ch4)
	for ti, tc := range []struct {
		ch             <-chan struct{}
		wantsRuleCount []int
	}{
		{ch1, []int{0, 0, 0, 0}},
		{ch2, []int{0, 0, 0, 0}},
		{ch3, []int{1, 1, 1, 1}},
		{ch4, []int{1, 2, 2, 2}},
	} {
		var (
			i, j int
			p    = prototype()
			r1   = counter(&i).Poll(tc.ch).Eval
			r2   = Rule(nil).Poll(tc.ch).Eval
		)
		for k, r := range []Rule{r1, r2} {
			for x := 0; x < 2; x++ {
				e, err := r(p, nil, chainCounter(&j, chainIdentity))
				if e != p {
					t.Errorf("test case %d failed: expected event %q instead of %q", ti, p, e)
				}
				if err != nil {
					t.Errorf("test case %d failed: unexpected error %v", ti, err)
				}
				if y := tc.wantsRuleCount[k*2+x]; i != y {
					t.Errorf("test case (%d,%d,%d) failed: expected count of %d instead of %d",
						ti, k, x, y, i)
				}
				if y := (k * 2) + x + 1; j != y {
					t.Errorf("test case %d failed: expected chain count of %d instead of %d",
						ti, y, j)
				}
			}
		}
	}
}
`))
