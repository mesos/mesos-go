package callrules

// go generate
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func prototype() *scheduler.Call { return &scheduler.Call{} }

func counter(i *int) Rule {
	return func(ctx context.Context, e *scheduler.Call, z mesos.Response, err error, ch Chain) (context.Context, *scheduler.Call, mesos.Response, error) {
		*i++
		return ch(ctx, e, z, err)
	}
}

func tracer(r Rule, name string, t *testing.T) Rule {
	return func(ctx context.Context, e *scheduler.Call, z mesos.Response, err error, ch Chain) (context.Context, *scheduler.Call, mesos.Response, error) {
		t.Log("executing", name)
		return r(ctx, e, z, err, ch)
	}
}

func returnError(re error) Rule {
	return func(ctx context.Context, e *scheduler.Call, z mesos.Response, err error, ch Chain) (context.Context, *scheduler.Call, mesos.Response, error) {
		return ch(ctx, e, z, Error2(err, re))
	}
}

func chainCounter(i *int, ch Chain) Chain {
	return func(ctx context.Context, e *scheduler.Call, z mesos.Response, err error) (context.Context, *scheduler.Call, mesos.Response, error) {
		*i++
		return ch(ctx, e, z, err)
	}
}

func TestChainIdentity(t *testing.T) {
	var i int
	counterRule := counter(&i)

	var z0 mesos.Response

	_, e, _, err := Rules{counterRule}.Eval(context.Background(), nil, z0, nil, chainIdentity)
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
		p   = prototype()
		a   = errors.New("a")
		ctx = context.Background()
	)

	var z0 mesos.Response
	var zp = &mesos.ResponseWrapper{}
	// multiple rules in Rules should execute, dropping nil rules along the way
	for _, tc := range []struct {
		e   *scheduler.Call
		z   mesos.Response
		err error
	}{
		{nil, z0, nil},
		{nil, z0, a},
		{p, z0, nil},
		{p, z0, a},

		{nil, zp, nil},
		{nil, zp, a},
		{p, zp, nil},
		{p, zp, a},
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
			_, e, zz, err = rule(ctx, tc.e, tc.z, tc.err, chainIdentity)
		)
		if e != tc.e {
			t.Errorf("expected prototype event %q instead of %q", tc.e, e)
		}
		if zz != tc.z {
			t.Errorf("expected return object %q instead of %q", tc.z, zz)
		}
		if err != tc.err {
			t.Errorf("expected %q error instead of %q", tc.err, err)
		}
		if i != 2 {
			t.Error("expected 2 rule executions instead of", i)
		}

		// empty Rules should not change event, z, err
		_, e, zz, err = Rules{}.Eval(ctx, tc.e, tc.z, tc.err, chainIdentity)
		if e != tc.e {
			t.Errorf("expected prototype event %q instead of %q", tc.e, e)
		}
		if zz != tc.z {
			t.Errorf("expected return object %q instead of %q", tc.z, zz)
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
		ctx  = context.Background()
		r1   = counter(&i)
		r2   = Rule(nil).AndThen(counter(&i))
		a    = errors.New("a")
	)
	var zp = &mesos.ResponseWrapper{}
	for k, r := range []Rule{r1, r2} {
		_, e, zz, err := r(ctx, p, zp, a, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if zz != zp {
			t.Errorf("expected return object %q instead of %q", zp, zz)
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

func TestOnFailure(t *testing.T) {
	var (
		i, j int
		p    = prototype()
		ctx  = context.Background()
		a    = errors.New("a")
		r1   = counter(&i)
		r2   = Fail(a).OnFailure(counter(&i))
	)
	var zp = &mesos.ResponseWrapper{}
	for k, tc := range []struct {
		r            Rule
		initialError error
	}{
		{r1, a},
		{r2, nil},
	} {
		_, e, zz, err := tc.r(ctx, p, zp, tc.initialError, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if zz != zp {
			t.Errorf("expected return object %q instead of %q", zp, zz)
		}
		if err != a {
			t.Error("unexpected error", err)
		}
		if i != (k + 1) {
			t.Errorf("expected count of %d instead of %d", (k + 1), i)
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
		ctx  = context.Background()
		r1   = counter(&i)
		r2   = counter(&i).DropOnError()
		a    = errors.New("a")
	)
	var zp = &mesos.ResponseWrapper{}
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for _, r := range []Rule{r1, r2} {
		_, e, zz, err := r(ctx, p, zp, a, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if zz != zp {
			t.Errorf("expected return object %q instead of %q", zp, zz)
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
	_, e, zz, err := r2(ctx, p, zp, nil, chainCounter(&j, chainIdentity))
	if e != p {
		t.Errorf("expected event %q instead of %q", p, e)
	}
	if zz != zp {
		t.Errorf("expected return object %q instead of %q", zp, zz)
	}
	if err != nil {
		t.Error("unexpected error", err)
	}
	if j != 2 {
		t.Errorf("expected chain count of 2 instead of %d", j)
	}
}

func TestDropOnSuccess(t *testing.T) {
	var (
		i, j int
		p    = prototype()
		ctx  = context.Background()
		r1   = counter(&i)
		r2   = counter(&i).DropOnSuccess()
	)
	var zp = &mesos.ResponseWrapper{}
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for _, r := range []Rule{r1, r2} {
		_, e, zz, err := r(ctx, p, zp, nil, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if zz != zp {
			t.Errorf("expected return object %q instead of %q", zp, zz)
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
	a := errors.New("a")
	_, e, zz, err := r2(ctx, p, zp, a, chainCounter(&j, chainIdentity))
	if e != p {
		t.Errorf("expected event %q instead of %q", p, e)
	}
	if zz != zp {
		t.Errorf("expected return object %q instead of %q", zp, zz)
	}
	if err != a {
		t.Error("unexpected error", err)
	}
	if i != 2 {
		t.Errorf("expected count of 2 instead of %d", i)
	}
	if j != 2 {
		t.Errorf("expected chain count of 2 instead of %d", j)
	}

	r3 := Rules{DropOnSuccess(), r1}.Eval
	_, e, zz, err = r3(ctx, p, zp, nil, chainCounter(&j, chainIdentity))
	if e != p {
		t.Errorf("expected event %q instead of %q", p, e)
	}
	if zz != zp {
		t.Errorf("expected return object %q instead of %q", zp, zz)
	}
	if err != nil {
		t.Error("unexpected error", err)
	}
	if i != 2 {
		t.Errorf("expected count of 2 instead of %d", i)
	}
	if j != 3 {
		t.Errorf("expected chain count of 3 instead of %d", j)
	}
}

func TestThenDrop(t *testing.T) {
	for _, anErr := range []error{nil, errors.New("a")} {
		var (
			i, j int
			p    = prototype()
			ctx  = context.Background()
			r1   = counter(&i)
			r2   = counter(&i).ThenDrop()
		)
		var zp = &mesos.ResponseWrapper{}
		// r1 and r2 should execute the counter rule
		for k, r := range []Rule{r1, r2} {
			_, e, zz, err := r(ctx, p, zp, anErr, chainCounter(&j, chainIdentity))
			if e != p {
				t.Errorf("expected event %q instead of %q", p, e)
			}
			if zz != zp {
				t.Errorf("expected return object %q instead of %q", zp, zz)
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
			ctx  = context.Background()
			r1   = counter(&i)
			r2   = Rules{Drop(), counter(&i)}.Eval
		)
		var zp = &mesos.ResponseWrapper{}
		// r1 should execute the counter rule
		// r2 should NOT exexute the counter rule
		for k, r := range []Rule{r1, r2} {
			_, e, zz, err := r(ctx, p, zp, anErr, chainCounter(&j, chainIdentity))
			if e != p {
				t.Errorf("expected event %q instead of %q", p, e)
			}
			if zz != zp {
				t.Errorf("expected return object %q instead of %q", zp, zz)
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
		ctx  = context.Background()
		r1   = counter(&i).If(true).Eval
		r2   = counter(&i).If(false).Eval
	)
	var zp = &mesos.ResponseWrapper{}
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for k, r := range []Rule{r1, r2} {
		_, e, zz, err := r(ctx, p, zp, nil, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if zz != zp {
			t.Errorf("expected return object %q instead of %q", zp, zz)
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
		ctx  = context.Background()
		r1   = counter(&i).Unless(false).Eval
		r2   = counter(&i).Unless(true).Eval
	)
	var zp = &mesos.ResponseWrapper{}
	// r1 should execute the counter rule
	// r2 should NOT exexute the counter rule
	for k, r := range []Rule{r1, r2} {
		_, e, zz, err := r(ctx, p, zp, nil, chainCounter(&j, chainIdentity))
		if e != p {
			t.Errorf("expected event %q instead of %q", p, e)
		}
		if zz != zp {
			t.Errorf("expected return object %q instead of %q", zp, zz)
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
		ctx  = context.Background()
		r1   = counter(&i).Once().Eval
		r2   = Rule(nil).Once().Eval
	)
	var zp = &mesos.ResponseWrapper{}
	for k, r := range []Rule{r1, r2} {
		for x := 0; x < 5; x++ {
			_, e, zz, err := r(ctx, p, zp, nil, chainCounter(&j, chainIdentity))
			if e != p {
				t.Errorf("expected event %q instead of %q", p, e)
			}
			if zz != zp {
				t.Errorf("expected return object %q instead of %q", zp, zz)
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
			ctx  = context.Background()
			r1   = counter(&i).Poll(tc.ch).Eval
			r2   = Rule(nil).Poll(tc.ch).Eval
		)
		var zp = &mesos.ResponseWrapper{}
		for k, r := range []Rule{r1, r2} {
			for x := 0; x < 2; x++ {
				_, e, zz, err := r(ctx, p, zp, nil, chainCounter(&j, chainIdentity))
				if e != p {
					t.Errorf("test case %d failed: expected event %q instead of %q", ti, p, e)
				}
				if zz != zp {
					t.Errorf("expected return object %q instead of %q", zp, zz)
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
