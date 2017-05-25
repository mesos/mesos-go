package eventrules

// go generate
// GENERATED CODE FOLLOWS; DO NOT EDIT.

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib/scheduler"
)

func counter(i *int) Rule {
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		*i++
		return ch(e, err)
	}
}

func returnError(re error) Rule {
	return func(e *scheduler.Event, err error, ch Chain) (*scheduler.Event, error) {
		return ch(e, Error2(err, re))
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

func TestError2(t *testing.T) {
	var (
		a = errors.New("a")
		b = errors.New("b")
	)
	for i, tc := range []struct {
		a     error
		b     error
		wants error
	}{
		{nil, nil, nil},
		{a, nil, a},
		{nil, b, b},
		{a, b, ErrorList{a, b}},
	} {
		result := Error2(tc.a, tc.b)
		// jump through hoops because we can't directly compare two errors with == if
		// they're both ErrorList.
		if IsErrorList(result) == IsErrorList(tc.wants) { // both are lists or neither
			if !IsErrorList(result) && result == tc.wants {
				continue
			}
			if IsErrorList(result) && reflect.DeepEqual(result, tc.wants) {
				continue
			}
		}
		t.Errorf("test case %d failed, expected %v instead of %v", i, tc.wants, result)
	}
}
