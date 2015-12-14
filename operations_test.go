package mesos_test

import (
	"testing"

	"github.com/mesos/mesos-go"
)

func TestOpReserve(t *testing.T) {
	// func opReserve(operation mesos.Offer_Operation, resources mesos.Resources) (mesos.Resources, error)
	var (
		unreservedCPU = resources(resource(name("cpus"), valueScalar(1)))
		unreservedMem = resources(resource(name("mem"), valueScalar(512)))
		unreserved    = unreservedCPU.PlusAll(unreservedMem)
		reservedCPU1  = unreservedCPU.Flatten("role", reservedBy("principal"))
	)

	// test case 1: reserve an amount of CPU that's available
	wantsReserved := unreservedMem.PlusAll(reservedCPU1)
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

func reservation(ri *mesos.Resource_ReservationInfo) resourceOpt {
	return func(r *mesos.Resource) {
		r.Reservation = ri
	}
}
