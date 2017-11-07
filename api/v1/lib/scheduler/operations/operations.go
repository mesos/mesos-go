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
	opErrorHandler  func(cause error) error
)

const (
	operationErrorTypeInvalid operationErrorType = iota
	operationErrorTypeUnknown
	operationErrorTypeMayNotBeApplied
)

var (
	opError = func(t mesos.Offer_Operation_Type) offerResourceOp {
		return func(_ *mesos.Offer_Operation, _ mesos.Resources, _ mesos.Resources) (_ mesos.Resources, err error) {
			err = mayNotBeApplied(t)
			return
		}
	}
	offerResourceOpMap = map[mesos.Offer_Operation_Type]offerResourceOp{
		mesos.Offer_Operation_LAUNCH:         opError(mesos.Offer_Operation_LAUNCH),
		mesos.Offer_Operation_LAUNCH_GROUP:   opError(mesos.Offer_Operation_LAUNCH_GROUP),
		mesos.Offer_Operation_RESERVE:        handleOpErrors(invalidOp(mesos.Offer_Operation_RESERVE), validateOpTotals(opReserve)),
		mesos.Offer_Operation_UNRESERVE:      handleOpErrors(invalidOp(mesos.Offer_Operation_UNRESERVE), validateOpTotals(opUnreserve)),
		mesos.Offer_Operation_CREATE:         handleOpErrors(invalidOp(mesos.Offer_Operation_CREATE), validateOpTotals(opCreate)),
		mesos.Offer_Operation_DESTROY:        handleOpErrors(invalidOp(mesos.Offer_Operation_DESTROY), validateOpTotals(opDestroy)),
		mesos.Offer_Operation_CREATE_VOLUME:  handleOpErrors(invalidOp(mesos.Offer_Operation_CREATE_VOLUME), validateOpTotals(opCreateVolume)),
		mesos.Offer_Operation_DESTROY_VOLUME: handleOpErrors(invalidOp(mesos.Offer_Operation_DESTROY_VOLUME), validateOpTotals(opDestroyVolume)),
		mesos.Offer_Operation_CREATE_BLOCK:   handleOpErrors(invalidOp(mesos.Offer_Operation_CREATE_BLOCK), validateOpTotals(opCreateBlock)),
		mesos.Offer_Operation_DESTROY_BLOCK:  handleOpErrors(invalidOp(mesos.Offer_Operation_DESTROY_BLOCK), validateOpTotals(opDestroyBlock)),
	}
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

func invalidOp(t mesos.Offer_Operation_Type) opErrorHandler {
	return func(cause error) error {
		return &operationError{errorType: operationErrorTypeInvalid, opType: t, cause: cause}
	}
}

func mayNotBeApplied(t mesos.Offer_Operation_Type) error {
	return &operationError{errorType: operationErrorTypeMayNotBeApplied, opType: t}
}

func handleOpErrors(f opErrorHandler, op offerResourceOp) offerResourceOp {
	return func(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
		result, err := op(operation, resources, conv)
		if err != nil {
			err = f(err)
		}
		return result, err
	}
}

func validateOpTotals(f offerResourceOp) offerResourceOp {
	return func(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
		result, err := f(operation, resources, conv)
		if err == nil {
			// sanity CHECK, same as apache/mesos does
			if !rez.SumAndCompare(resources, result...) {
				panic(fmt.Sprintf("result %+v != resources %+v", result, resources))
			}
		}
		return result, err
	}
}

func opReserve(operation *mesos.Offer_Operation, resources mesos.Resources, conv mesos.Resources) (mesos.Resources, error) {
	if len(conv) > 0 {
		return nil, fmt.Errorf("converted resources not expected")
	}
	result := resources.Clone()
	opRes := operation.GetReserve().GetResources()
	err := rez.Validate(opRes...)
	if err != nil {
		return nil, err
	}
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
	if len(conv) > 0 {
		return nil, fmt.Errorf("converted resources not expected")
	}
	result := resources.Clone()
	opRes := operation.GetUnreserve().GetResources()
	err := rez.Validate(opRes...)
	if err != nil {
		return nil, err
	}
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
	if len(conv) > 0 {
		return nil, fmt.Errorf("converted resources not expected")
	}
	result := resources.Clone()
	volumes := operation.GetCreate().GetVolumes()
	err := rez.Validate(volumes...)
	if err != nil {
		return nil, err
	}
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
	if len(conv) > 0 {
		return nil, fmt.Errorf("converted resources not expected")
	}
	result := resources.Clone()
	volumes := operation.GetDestroy().GetVolumes()
	err := rez.Validate(volumes...)
	if err != nil {
		return nil, err
	}
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

func Apply(operation *mesos.Offer_Operation, resources []mesos.Resource, convertedResources []mesos.Resource) ([]mesos.Resource, error) {
	f, ok := offerResourceOpMap[operation.GetType()]
	if !ok {
		return nil, &operationError{
			errorType: operationErrorTypeUnknown,
			cause:     errors.New("unknown offer operation: " + operation.GetType().String()),
		}
	}
	return f(operation, resources, convertedResources)
}
