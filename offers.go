package mesos

import (
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
)

type offerResourceOp func(Offer_Operation, Resources) (Resources, error)

var (
	opNoop             = func(_ Offer_Operation, _ Resources) (_ Resources, _ error) { return }
	offerResourceOpMap = map[Offer_Operation_Type]offerResourceOp{
		LAUNCH:    opNoop,
		RESERVE:   validateOpTotals(opReserve),
		UNRESERVE: validateOpTotals(opUnreserve),
		CREATE:    validateOpTotals(opCreate),
		DESTROY:   validateOpTotals(opDestroy),
	}
)

func validateOpTotals(f offerResourceOp) offerResourceOp {
	return func(operation Offer_Operation, resources Resources) (Resources, error) {
		result, err := f(operation, resources)
		if err == nil {
			// sanity CHECK, same as apache/mesos does
			if !resources.sameTotals(result) {
				panic("result != resources")
			}
		}
		return result, err
	}
}

func opReserve(operation Offer_Operation, resources Resources) (Resources, error) {
	result := resources.Clone()
	opRes := Resources(operation.GetReserve().GetResources())
	err := opRes.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid RESERVE operation: %+v", err)
	}
	for _, r := range opRes {
		if !r.IsReserved("") {
			return nil, errors.New("invalid RESERVE operation: Resource must be reserved")
		}
		if r.GetReservation() == nil {
			return nil, errors.New("invalid RESERVE operation: missing 'reservation'")
		}
		unreserved := Resources{r}.Flatten("", nil)
		if !result.ContainsAll(unreserved) {
			return nil, fmt.Errorf("invalid RESERVE operation: %+v does not contain %+v", result, unreserved)
		}
		result.SubtractAll(unreserved)
		result.Add(r)
	}
	return result, nil
}

func opUnreserve(operation Offer_Operation, resources Resources) (Resources, error) {
	result := resources.Clone()
	opRes := Resources(operation.GetUnreserve().GetResources())
	err := opRes.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid UNRESERVE operation: %+v", err)
	}
	for _, r := range opRes {
		if !r.IsReserved("") {
			return nil, errors.New("invalid UNRESERVE operation: Resource is not reserved")
		}
		if r.GetReservation() == nil {
			return nil, errors.New("invalid UNRESERVE operation: missing 'reservation'")
		}
		unreserved := Resources{r}.Flatten("", nil)
		result.Subtract(r)
		result.AddAll(unreserved)
	}
	return result, nil
}

func opCreate(operation Offer_Operation, resources Resources) (Resources, error) {
	result := resources.Clone()
	volumes := Resources(operation.GetCreate().GetVolumes())
	err := volumes.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid CREATE operation: %+v", err)
	}
	for _, v := range volumes {
		if v.GetDisk() == nil {
			return nil, errors.New("invalid CREATE operation: missing 'disk'")
		}
		if v.GetDisk().GetPersistence() == nil {
			return nil, errors.New("invalid CREATE operation: missing 'persistence'")
		}
		// from: https://github.com/apache/mesos/blob/master/src/common/resources.cpp
		// Strip the disk info so that we can subtract it from the
		// original resources.
		// TODO(jieyu): Non-persistent volumes are not supported for
		// now. Persistent volumes can only be be created from regular
		// disk resources. Revisit this once we start to support
		// non-persistent volumes.
		stripped := proto.Clone(&v).(*Resource)
		stripped.Disk = nil
		if !result.Contains(*stripped) {
			return nil, errors.New("invalid CREATE operation: insufficient disk resources")
		}
		result.Subtract(*stripped)
		result.Add(v)
	}
	return result, nil
}

func opDestroy(operation Offer_Operation, resources Resources) (Resources, error) {
	result := resources.Clone()
	volumes := Resources(operation.GetDestroy().GetVolumes())
	err := volumes.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid DESTROY operation: %+v", err)
	}
	for _, v := range volumes {
		if v.GetDisk() == nil {
			return nil, errors.New("invalid DESTROY operation: missing 'disk'")
		}
		if v.GetDisk().GetPersistence() == nil {
			return nil, errors.New("invalid DESTROY operation: missing 'persistence'")
		}
		if !result.Contains(v) {
			return nil, errors.New("invalid DESTROY operation: persistent volume does not exist")
		}
		stripped := proto.Clone(&v).(*Resource)
		stripped.Disk = nil
		result.Subtract(v)
		result.Add(*stripped)
	}
	return result, nil
}

func (operation Offer_Operation) Apply(resources Resources) (Resources, error) {
	f, ok := offerResourceOpMap[operation.GetType()]
	if !ok {
		return nil, errors.New("unknown offer operation: " + operation.GetType().String())
	}
	return f(operation, resources)
}
