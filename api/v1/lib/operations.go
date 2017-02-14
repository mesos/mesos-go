package mesos

import (
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
)

type (
	OperationErrorType int

	OperationError struct {
		errorType OperationErrorType
		opType    Offer_Operation_Type
		cause     error
	}

	offerResourceOp func(Offer_Operation, Resources) (Resources, error)
	opErrorHandler  func(cause error) error
)

const (
	OperationErrorTypeInvalid OperationErrorType = iota
	OperationErrorTypeUnknown
)

var (
	opNoop             = func(_ Offer_Operation, _ Resources) (_ Resources, _ error) { return }
	offerResourceOpMap = map[Offer_Operation_Type]offerResourceOp{
		LAUNCH:    opNoop,
		RESERVE:   handleOpErrors(invalidOp(RESERVE), validateOpTotals(opReserve)),
		UNRESERVE: handleOpErrors(invalidOp(UNRESERVE), validateOpTotals(opUnreserve)),
		CREATE:    handleOpErrors(invalidOp(CREATE), validateOpTotals(opCreate)),
		DESTROY:   handleOpErrors(invalidOp(DESTROY), validateOpTotals(opDestroy)),
	}
)

func (err *OperationError) Cause() error                    { return err.cause }
func (err *OperationError) Type() OperationErrorType        { return err.errorType }
func (err *OperationError) Operation() Offer_Operation_Type { return err.opType }

func (err *OperationError) Error() string {
	switch err.errorType {
	case OperationErrorTypeInvalid:
		return fmt.Sprintf("invalid "+err.opType.String()+" operation: %+v", err.cause)
	case OperationErrorTypeUnknown:
		return err.cause.Error()
	default:
		return fmt.Sprintf("operation error: %+v", err.cause)
	}
}

func invalidOp(t Offer_Operation_Type) opErrorHandler {
	return func(cause error) error {
		return &OperationError{errorType: OperationErrorTypeInvalid, opType: t, cause: cause}
	}
}

func handleOpErrors(f opErrorHandler, op offerResourceOp) offerResourceOp {
	return func(operation Offer_Operation, resources Resources) (Resources, error) {
		result, err := op(operation, resources)
		if err != nil {
			err = f(err)
		}
		return result, err
	}
}

func validateOpTotals(f offerResourceOp) offerResourceOp {
	return func(operation Offer_Operation, resources Resources) (Resources, error) {
		result, err := f(operation, resources)
		if err == nil {
			// sanity CHECK, same as apache/mesos does
			if !resources.sameTotals(result) {
				panic(fmt.Sprintf("result %+v != resources %+v", result, resources))
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
		return nil, err
	}
	for i := range opRes {
		if !opRes[i].IsReserved("") {
			return nil, errors.New("Resource must be reserved")
		}
		if opRes[i].GetReservation() == nil {
			return nil, errors.New("missing 'reservation'")
		}
		unreserved := Resources{opRes[i]}.Flatten()
		if !result.ContainsAll(unreserved) {
			return nil, fmt.Errorf("%+v does not contain %+v", result, unreserved)
		}
		result.Subtract(unreserved...)
		result.add(opRes[i])
	}
	return result, nil
}

func opUnreserve(operation Offer_Operation, resources Resources) (Resources, error) {
	result := resources.Clone()
	opRes := Resources(operation.GetUnreserve().GetResources())
	err := opRes.Validate()
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
		if !result.Contains(opRes[i]) {
			return nil, errors.New("resources do not contain unreserve amount") //TODO(jdef) should output nicely formatted resource quantities here
		}
		unreserved := Resources{opRes[i]}.Flatten()
		result.subtract(opRes[i])
		result.Add(unreserved...)
	}
	return result, nil
}

func opCreate(operation Offer_Operation, resources Resources) (Resources, error) {
	result := resources.Clone()
	volumes := Resources(operation.GetCreate().GetVolumes())
	err := volumes.Validate()
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
		stripped := proto.Clone(&volumes[i]).(*Resource)
		stripped.Disk = nil
		if !result.Contains(*stripped) {
			return nil, errors.New("invalid CREATE operation: insufficient disk resources")
		}
		result.subtract(*stripped)
		result.add(volumes[i])
	}
	return result, nil
}

func opDestroy(operation Offer_Operation, resources Resources) (Resources, error) {
	result := resources.Clone()
	volumes := Resources(operation.GetDestroy().GetVolumes())
	err := volumes.Validate()
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
		if !result.Contains(volumes[i]) {
			return nil, errors.New("persistent volume does not exist")
		}
		stripped := proto.Clone(&volumes[i]).(*Resource)
		stripped.Disk = nil
		result.subtract(volumes[i])
		result.add(*stripped)
	}
	return result, nil
}

func (operation Offer_Operation) Apply(resources Resources) (Resources, error) {
	f, ok := offerResourceOpMap[operation.GetType()]
	if !ok {
		return nil, &OperationError{
			errorType: OperationErrorTypeUnknown,
			cause:     errors.New("unknown offer operation: " + operation.GetType().String()),
		}
	}
	return f(operation, resources)
}
