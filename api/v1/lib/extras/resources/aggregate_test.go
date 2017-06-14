package resources_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib"
	rez "github.com/mesos/mesos-go/api/v1/lib/extras/resources"
	. "github.com/mesos/mesos-go/api/v1/lib/resourcetest"
)

func TestResources_Types(t *testing.T) {
	rs := Resources(
		Resource(Name("cpus"), ValueScalar(2), Role("role1")),
		Resource(Name("cpus"), ValueScalar(4)),
		Resource(Name("ports"), ValueRange(Span(1, 10)), Role("role1")),
		Resource(Name("ports"), ValueRange(Span(11, 20))),
	)
	types := rez.Types(rs...)
	expected := map[string]mesos.Value_Type{
		"cpus":  mesos.SCALAR,
		"ports": mesos.RANGES,
	}
	if !reflect.DeepEqual(types, expected) {
		t.Fatalf("expected %v instead of %v", expected, types)
	}
}

func TestResources_Names(t *testing.T) {
	rs := Resources(
		Resource(Name("cpus"), ValueScalar(2), Role("role1")),
		Resource(Name("cpus"), ValueScalar(4)),
		Resource(Name("mem"), ValueScalar(10), Role("role1")),
		Resource(Name("mem"), ValueScalar(10)),
	)
	names := rez.Names(rs...)
	sort.Strings(names)
	expected := []string{"cpus", "mem"}
	if !reflect.DeepEqual(names, expected) {
		t.Fatalf("expected %v instead of %v", expected, names)
	}
}
