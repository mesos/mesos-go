package operations

import (
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/api/v1/lib"
	rez "github.com/mesos/mesos-go/api/v1/lib/resources"
)

type (
	operationErrorType int

	operationError struct {
		errorType operationErrorType
		opType    mesos.Offer_Operation_Type
		cause     error
	}

	offerResourceOp func(_ *mesos.Offer_Operation, res mesos.Resources, converted mesos.Resources) (mesos.Resources, error)
)

const (
	operationErrorTypeInvalid operationErrorType = iota
	operationErrorTypeUnknown
	operationErrorTypeMayNotBeApplied
)

var (
	offerResourceOpMap = func() (m map[mesos.Offer_Operation_Type]offerResourceOp) {
		opUnsupported := func(t mesos.Offer_Operation_Type) {
			m[t] = func(_ *mesos.Offer_Operation, _ mesos.Resources, _ mesos.Resources) (_ mesos.Resources, err error) {
				err = &operationError{errorType: operationErrorTypeMayNotBeApplied, opType: t}
				return
			}
		}
		invalidOp := func(t mesos.Offer_Operation_Type, op offerResourceOp) offerResourceOp {
			return func(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
				r, err := op(operation, resources, conv)
				if err != nil {
					err = &operationError{errorType: operationErrorTypeInvalid, opType: t, cause: err}
				}
				return r, err
			}
		}
		opRegister := func(t mesos.Offer_Operation_Type, op offerResourceOp) { m[t] = invalidOp(t, op) }

		m = make(map[mesos.Offer_Operation_Type]offerResourceOp)

		opUnsupported(mesos.Offer_Operation_LAUNCH)
		opUnsupported(mesos.Offer_Operation_LAUNCH_GROUP)

		// TODO(jdef) does it make sense to validate op totals for all operation types?

		opRegister(mesos.Offer_Operation_RESERVE, opReserve)
		opRegister(mesos.Offer_Operation_UNRESERVE, opUnreserve)
		opRegister(mesos.Offer_Operation_CREATE, opCreate)
		opRegister(mesos.Offer_Operation_DESTROY, opDestroy)
		opRegister(mesos.Offer_Operation_CREATE_VOLUME, opCreateVolume)
		opRegister(mesos.Offer_Operation_DESTROY_VOLUME, opDestroyVolume)
		opRegister(mesos.Offer_Operation_CREATE_BLOCK, opCreateBlock)
		opRegister(mesos.Offer_Operation_DESTROY_BLOCK, opDestroyBlock)

		return
	}()
)

func (err *operationError) Cause() error                          { return err.cause }
func (err *operationError) Type() operationErrorType              { return err.errorType }
func (err *operationError) Operation() mesos.Offer_Operation_Type { return err.opType }

func (err *operationError) Error() string {
	switch err.errorType {
	case operationErrorTypeInvalid:
		return fmt.Sprintf("invalid "+err.opType.String()+" operation: %+v", err.cause)
	case operationErrorTypeUnknown:
		return err.cause.Error()
	default:
		return fmt.Sprintf("operation error: %+v", err.cause)
	}
}

func opReserve(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	// TODO(jdef) sync this w/ mesos: consider reservation refinements and shared resources
	if len(conv) > 0 {
		return nil, fmt.Errorf("converted resources not expected")
	}
	opRes := operation.GetReserve().GetResources()
	err := rez.Validate(opRes...)
	if err != nil {
		return nil, err
	}
	result := resources.Clone()
	for i := range opRes {
		if !opRes[i].IsReserved("") {
			return nil, errors.New("Resource must be reserved")
		}
		if opRes[i].GetReservation() == nil {
			return nil, errors.New("missing 'reservation'")
		}
		unreserved := rez.Flatten(mesos.Resources{opRes[i]})
		if !rez.ContainsAll(result, unreserved) {
			return nil, fmt.Errorf("%+v does not contain %+v", result, unreserved)
		}
		result.Subtract(unreserved...)
		result.Add1(opRes[i])
	}
	return result, nil
}

func opUnreserve(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	// TODO(jdef) sync this w/ mesos: consider reservation refinements and shared resources
	if len(conv) > 0 {
		return nil, fmt.Errorf("converted resources not expected")
	}
	opRes := operation.GetUnreserve().GetResources()
	err := rez.Validate(opRes...)
	if err != nil {
		return nil, err
	}
	result := resources.Clone()
	for i := range opRes {
		if !opRes[i].IsReserved("") {
			return nil, errors.New("Resource is not reserved")
		}
		if opRes[i].GetReservation() == nil {
			return nil, errors.New("missing 'reservation'")
		}
		if !rez.Contains(result, opRes[i]) {
			return nil, errors.New("resources do not contain unreserve amount") //TODO(jdef) should output nicely formatted resource quantities here
		}
		unreserved := rez.Flatten(mesos.Resources{opRes[i]})
		result.Subtract1(opRes[i])
		result.Add(unreserved...)
	}
	return result, nil
}

func opCreate(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	// TODO(jdef) sync this w/ mesos: consider reservation refinements and shared resources
	if len(conv) > 0 {
		return nil, fmt.Errorf("converted resources not expected")
	}
	volumes := operation.GetCreate().GetVolumes()
	err := rez.Validate(volumes...)
	if err != nil {
		return nil, err
	}
	result := resources.Clone()
	for i := range volumes {
		if volumes[i].GetDisk() == nil {
			return nil, errors.New("missing 'disk'")
		}
		if volumes[i].GetDisk().GetPersistence() == nil {
			return nil, errors.New("missing 'persistence'")
		}
		// from: https://github.com/apache/mesos/blob/master/src/common/resources.cpp
		// Strip the disk info so that we can subtract it from the
		// original resources.
		// TODO(jieyu): Non-persistent volumes are not supported for
		// now. Persistent volumes can only be be created from regular
		// disk resources. Revisit this once we start to support
		// non-persistent volumes.
		stripped := proto.Clone(&volumes[i]).(*mesos.Resource)
		stripped.Disk = nil
		if !rez.Contains(result, *stripped) {
			return nil, errors.New("invalid CREATE operation: insufficient disk resources")
		}
		result.Subtract1(*stripped)
		result.Add1(volumes[i])
	}
	return result, nil
}

func opDestroy(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	// TODO(jdef) sync this w/ mesos: consider reservation refinements and shared resources
	if len(conv) > 0 {
		return nil, fmt.Errorf("converted resources not expected")
	}
	volumes := operation.GetDestroy().GetVolumes()
	err := rez.Validate(volumes...)
	if err != nil {
		return nil, err
	}
	result := resources.Clone()
	for i := range volumes {
		if volumes[i].GetDisk() == nil {
			return nil, errors.New("missing 'disk'")
		}
		if volumes[i].GetDisk().GetPersistence() == nil {
			return nil, errors.New("missing 'persistence'")
		}
		if !rez.Contains(result, volumes[i]) {
			return nil, errors.New("persistent volume does not exist")
		}
		stripped := proto.Clone(&volumes[i]).(*mesos.Resource)
		stripped.Disk = nil
		result.Subtract1(volumes[i])
		result.Add1(*stripped)
	}
	return result, nil
}

func opCreateVolume(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	if len(conv) == 0 {
		return nil, fmt.Errorf("converted resources not specified")
	}

	result := resources.Clone()
	consumed := operation.GetCreateVolume().GetSource()

	if !rez.Contains(result, consumed) {
		return nil, fmt.Errorf("%q does not contain %q", result, consumed)
	}

	result.Subtract1(consumed)
	result.Add(conv...)

	return result, nil
}

func opDestroyVolume(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	if len(conv) == 0 {
		return nil, fmt.Errorf("converted resources not specified")
	}

	result := resources.Clone()
	consumed := operation.GetDestroyVolume().GetVolume()

	if !rez.Contains(result, consumed) {
		return nil, fmt.Errorf("%q does not contain %q", result, consumed)
	}

	result.Subtract1(consumed)
	result.Add(conv...)

	return result, nil
}

func opCreateBlock(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	if len(conv) == 0 {
		return nil, fmt.Errorf("converted resources not specified")
	}

	result := resources.Clone()
	consumed := operation.GetCreateBlock().GetSource()

	if !rez.Contains(result, consumed) {
		return nil, fmt.Errorf("%q does not contain %q", result, consumed)
	}

	result.Subtract1(consumed)
	result.Add(conv...)

	return result, nil
}

func opDestroyBlock(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	if len(conv) == 0 {
		return nil, fmt.Errorf("converted resources not specified")
	}

	result := resources.Clone()
	consumed := operation.GetDestroyBlock().GetBlock()

	if !rez.Contains(result, consumed) {
		return nil, fmt.Errorf("%q does not contain %q", result, consumed)
	}

	result.Subtract1(consumed)
	result.Add(conv...)

	return result, nil
}

func Apply(operation *mesos.Offer_Operation, resources []mesos.Resource, convertedResources []mesos.Resource) (result []mesos.Resource, err error) {
	f, ok := offerResourceOpMap[operation.GetType()]
	if !ok {
		return nil, &operationError{
			errorType: operationErrorTypeUnknown,
			cause:     errors.New("unknown offer operation: " + operation.GetType().String()),
		}
	}
	result, err = f(operation, resources, convertedResources)
	if err == nil {
		// sanity CHECK, same as apache/mesos does
		if !rez.SumAndCompare(resources, result...) {
			panic(fmt.Sprintf("result %+v != resources %+v", result, resources))
		}
	}
	return result, err
}
