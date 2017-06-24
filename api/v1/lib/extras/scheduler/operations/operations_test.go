package operations_test

import (
	"testing"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/extras/scheduler/operations"
	rez "github.com/mesos/mesos-go/api/v1/lib/resources"
	. "github.com/mesos/mesos-go/api/v1/lib/resourcetest"
)

func TestOpCreate(t *testing.T) {
	var (
		total = Resources(
			Resource(Name("cpus"), ValueScalar(1)),
			Resource(Name("mem"), ValueScalar(512)),
			Resource(Name("disk"), ValueScalar(1000), Role("role")),
		)
		volume1 = Resource(Name("disk"), ValueScalar(200), Role("role"), Disk("1", "path"))
		volume2 = Resource(Name("disk"), ValueScalar(2000), Role("role"), Disk("1", "path"))
	)
	op := Create(Resources(volume1))
	rs, err := operations.Apply(op, total)
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	expected := Resources(
		Resource(Name("cpus"), ValueScalar(1)),
		Resource(Name("mem"), ValueScalar(512)),
		Resource(Name("disk"), ValueScalar(800), Role("role")),
		volume1,
	)
	if !rez.Equivalent(expected, rs) {
		t.Fatalf("expected %v instead of %v", expected, rs)
	}

	// check the case of insufficient disk resources
	op = Create(Resources(volume2))
	_, err = operations.Apply(op, total)
	if err == nil {
		t.Fatalf("expected an error due to insufficient disk resources")
	}
}

func TestOpUnreserve(t *testing.T) {
	var (
		reservedCPU = Resources(
			Resource(Name("cpus"),
				ValueScalar(1),
				Role("role"),
				Reservation(ReservedBy("principal"))))
		reservedMem = Resources(
			Resource(Name("mem"),
				ValueScalar(512),
				Role("role"),
				Reservation(ReservedBy("principal"))))
		reserved = reservedCPU.Plus(reservedMem...)
	)

	// test case 1: unreserve some amount of CPU that's already been reserved
	unreservedCPU := rez.Flatten(reservedCPU)
	t.Log("unreservedCPU=" + mesos.Resources(unreservedCPU).String())

	wantsUnreserved := reservedMem.Plus(unreservedCPU...)
	actualUnreserved, err := operations.Apply(Unreserve(reservedCPU), reserved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rez.Equivalent(wantsUnreserved, actualUnreserved) {
		t.Errorf("expected resources %+v instead of %+v", wantsUnreserved, actualUnreserved)
	}

	// test case 2: unreserve some amount of CPU greater than that which already been reserved
	reservedCPU2 := Resources(
		Resource(Name("cpus"),
			ValueScalar(2),
			Role("role"),
			Reservation(ReservedBy("principal"))))
	_, err = operations.Apply(Unreserve(reservedCPU2), reserved)
	if err == nil {
		t.Fatalf("expected reservation error")
	}
}

func TestOpReserve(t *testing.T) {
	// func opReserve(operation mesos.Offer_Operation, resources mesos.Resources) (mesos.Resources, error)
	var (
		unreservedCPU = Resources(Resource(Name("cpus"), ValueScalar(1)))
		unreservedMem = Resources(Resource(Name("mem"), ValueScalar(512)))
		unreserved    = unreservedCPU.Plus(unreservedMem...)
		reservedCPU1  = rez.Flatten(unreservedCPU, rez.Role("role").Assign(), ReservedBy("principal").Assign())
	)

	// test case 1: reserve an amount of CPU that's available
	wantsReserved := unreservedMem.Plus(reservedCPU1...)
	actualReserved, err := operations.Apply(Reserve(reservedCPU1), unreserved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rez.Equivalent(wantsReserved, actualReserved) {
		t.Errorf("expected resources %+v instead of %+v", wantsReserved, actualReserved)
	}

	// test case 2: reserve an amount of CPU that's NOT available
	reservedCPU2 := Resources(
		Resource(Name("cpus"),
			ValueScalar(2),
			Role("role"),
			Reservation(ReservedBy("principal"))))
	_, err = operations.Apply(Reserve(reservedCPU2), unreserved)
	if err == nil {
		t.Fatalf("expected reservation error")
	}
}
