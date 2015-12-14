package mesos_test

import (
	"testing"

	"github.com/mesos/mesos-go"
)

func TestOpUnreserve(t *testing.T) {
	var (
		reservedCPU = resources(
			resource(name("cpus"),
				valueScalar(1),
				role("role"),
				reservation(reservedBy("principal"))))
		reservedMem = resources(
			resource(name("mem"),
				valueScalar(512),
				role("role"),
				reservation(reservedBy("principal"))))
		reserved = reservedCPU.Plus(reservedMem...)
	)

	// test case 1: unreserve some amount of CPU that's already been reserved
	unreservedCPU := reservedCPU.Flatten("", nil)
	wantsUnreserved := reservedMem.Plus(unreservedCPU...)
	actualUnreserved, err := unreserve(reservedCPU).Apply(reserved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !wantsUnreserved.Equivalent(actualUnreserved) {
		t.Errorf("expected resources %+v instead of %+v", wantsUnreserved, actualUnreserved)
	}

	// test case 2: unreserve some amount of CPU greater than that which already been reserved
	reservedCPU2 := resources(
		resource(name("cpus"),
			valueScalar(2),
			role("role"),
			reservation(reservedBy("principal"))))
	_, err = unreserve(reservedCPU2).Apply(reserved)
	if err == nil {
		t.Fatalf("expected reservation error")
	}
}

func TestOpReserve(t *testing.T) {
	// func opReserve(operation mesos.Offer_Operation, resources mesos.Resources) (mesos.Resources, error)
	var (
		unreservedCPU = resources(resource(name("cpus"), valueScalar(1)))
		unreservedMem = resources(resource(name("mem"), valueScalar(512)))
		unreserved    = unreservedCPU.Plus(unreservedMem...)
		reservedCPU1  = unreservedCPU.Flatten("role", reservedBy("principal"))
	)

	// test case 1: reserve an amount of CPU that's available
	wantsReserved := unreservedMem.Plus(reservedCPU1...)
	actualReserved, err := reserve(reservedCPU1).Apply(unreserved)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !wantsReserved.Equivalent(actualReserved) {
		t.Errorf("expected resources %+v instead of %+v", wantsReserved, actualReserved)
	}

	// test case 2: reserve an amount of CPU that's NOT available
	reservedCPU2 := resources(
		resource(name("cpus"),
			valueScalar(2),
			role("role"),
			reservation(reservedBy("principal"))))
	_, err = reserve(reservedCPU2).Apply(unreserved)
	if err == nil {
		t.Fatalf("expected reservation error")
	}
}

func reservedBy(principal string) *mesos.Resource_ReservationInfo {
	return &mesos.Resource_ReservationInfo{
		Principal: principal,
	}
}

func reserve(r mesos.Resources) *mesos.Offer_Operation {
	return &mesos.Offer_Operation{
		Type: mesos.RESERVE.Enum(),
		Reserve: &mesos.Offer_Operation_Reserve{
			Resources: r,
		},
	}
}

func unreserve(r mesos.Resources) *mesos.Offer_Operation {
	return &mesos.Offer_Operation{
		Type: mesos.UNRESERVE.Enum(),
		Unreserve: &mesos.Offer_Operation_Unreserve{
			Resources: r,
		},
	}
}

func reservation(ri *mesos.Resource_ReservationInfo) resourceOpt {
	return func(r *mesos.Resource) {
		r.Reservation = ri
	}
}
