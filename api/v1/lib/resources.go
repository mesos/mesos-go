package mesos

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/mesos/mesos-go/api/v1/lib/roles"
)

type (
	Resources         []Resource
	resourceErrorType int

	resourceError struct {
		errorType resourceErrorType
		reason    string
		spec      Resource
	}
)

const (
	resourceErrorTypeIllegalName resourceErrorType = iota
	resourceErrorTypeIllegalType
	resourceErrorTypeUnsupportedType
	resourceErrorTypeIllegalScalar
	resourceErrorTypeIllegalRanges
	resourceErrorTypeIllegalSet
	resourceErrorTypeIllegalDisk
	resourceErrorTypeIllegalReservation
	resourceErrorTypeIllegalShare

	noReason = "" // make error generation code more readable
)

var (
	resourceErrorMessages = map[resourceErrorType]string{
		resourceErrorTypeIllegalName:        "missing or illegal resource name",
		resourceErrorTypeIllegalType:        "missing or illegal resource type",
		resourceErrorTypeUnsupportedType:    "unsupported resource type",
		resourceErrorTypeIllegalScalar:      "illegal scalar resource",
		resourceErrorTypeIllegalRanges:      "illegal ranges resource",
		resourceErrorTypeIllegalSet:         "illegal set resource",
		resourceErrorTypeIllegalDisk:        "illegal disk resource",
		resourceErrorTypeIllegalReservation: "illegal resource reservation",
		resourceErrorTypeIllegalShare:       "illegal shared resource",
	}
)

func (t resourceErrorType) Generate(reason string) error {
	msg := resourceErrorMessages[t]
	if reason != noReason {
		if msg != "" {
			msg += ": " + reason
		} else {
			msg = reason
		}
	}
	return &resourceError{errorType: t, reason: msg}
}

func (err *resourceError) Reason() string          { return err.reason }
func (err *resourceError) Resource() Resource      { return err.spec }
func (err *resourceError) WithResource(r Resource) { err.spec = r }

func (err *resourceError) Error() string {
	// TODO(jdef) include additional context here? (type, resource)
	if err.reason != "" {
		return "resource error: " + err.reason
	}
	return "resource error"
}

func IsResourceError(err error) (ok bool) {
	_, ok = err.(*resourceError)
	return
}

func (r *Resource_ReservationInfo) Assign() func(interface{}) {
	return func(v interface{}) {
		type reserver interface {
			WithReservation(*Resource_ReservationInfo)
		}
		if ri, ok := v.(reserver); ok {
			ri.WithReservation(r)
		}
	}
}

func (resources Resources) Clone() Resources {
	if resources == nil {
		return nil
	}
	clone := make(Resources, 0, len(resources))
	for i := range resources {
		rr := proto.Clone(&resources[i]).(*Resource)
		clone = append(clone, *rr)
	}
	return clone
}

// Minus calculates and returns the result of `resources - that` without modifying either
// the receiving `resources` or `that`.
func (resources Resources) Minus(that ...Resource) Resources {
	x := resources.Clone()
	return x.Subtract(that...)
}

// Subtract subtracts `that` from the receiving `resources` and returns the result (the modified
// `resources` receiver).
func (resources *Resources) Subtract(that ...Resource) (rs Resources) {
	if resources != nil {
		if len(that) > 0 {
			x := make(Resources, len(that))
			copy(x, that)
			that = x

			for i := range that {
				resources.Subtract1(that[i])
			}
		}
		rs = *resources
	}
	return
}

// Plus calculates and returns the result of `resources + that` without modifying either
// the receiving `resources` or `that`.
func (resources Resources) Plus(that ...Resource) Resources {
	x := resources.Clone()
	return x.Add(that...)
}

// Add adds `that` to the receiving `resources` and returns the result (the modified
// `resources` receiver).
func (resources *Resources) Add(that ...Resource) (rs Resources) {
	if resources != nil {
		rs = *resources
	}
	for i := range that {
		rs = rs._add(that[i])
	}
	if resources != nil {
		*resources = rs
	}
	return
}

// Add1 adds `that` to the receiving `resources` and returns the result (the modified
// `resources` receiver).
func (resources *Resources) Add1(that Resource) (rs Resources) {
	if resources != nil {
		rs = *resources
	}
	rs = rs._add(that)
	if resources != nil {
		*resources = rs
	}
	return
}

func (resources Resources) _add(that Resource) Resources {
	if that.Validate() != nil || that.IsEmpty() {
		return resources
	}
	for i := range resources {
		r := &resources[i]
		if r.Addable(that) {
			r.Add(that)
			return resources
		}
	}
	// cannot be combined with an existing resource
	r := proto.Clone(&that).(*Resource)
	return append(resources, *r)
}

// Minus1 calculates and returns the result of `resources - that` without modifying either
// the receiving `resources` or `that`.
func (resources *Resources) Minus1(that Resource) Resources {
	x := resources.Clone()
	return x.Subtract1(that)
}

// Subtract1 subtracts `that` from the receiving `resources` and returns the result (the modified
// `resources` receiver).
func (resources *Resources) Subtract1(that Resource) Resources {
	if resources == nil {
		return nil
	}
	if that.Validate() == nil && !that.IsEmpty() {
		for i := range *resources {
			r := &(*resources)[i]
			if r.Subtractable(that) {
				r.Subtract(that)
				// remove the resource if it becomes invalid or zero.
				// need to do validation in order to strip negative scalar
				// resource objects.
				if r.Validate() != nil || r.IsEmpty() {
					// delete resource at i, without leaking an uncollectable Resource
					// a, a[len(a)-1] = append(a[:i], a[i+1:]...), nil
					(*resources), (*resources)[len((*resources))-1] = append((*resources)[:i], (*resources)[i+1:]...), Resource{}
				}
				break
			}
		}
	}
	return *resources
}

func (resources Resources) String() string {
	if len(resources) == 0 {
		return ""
	}
	buf := bytes.Buffer{}
	for i := range resources {
		if i > 0 {
			buf.WriteString(";")
		}
		r := &resources[i]
		buf.WriteString(r.Name)
		buf.WriteString("(")
		buf.WriteString(r.GetRole())
		if ri := r.GetReservation(); ri != nil {
			buf.WriteString(", ")
			buf.WriteString(ri.GetPrincipal())
		}
		buf.WriteString(")")
		if d := r.GetDisk(); d != nil {
			buf.WriteString("[")
			if s := d.GetSource(); s != nil {
				switch s.GetType() {
				case Resource_DiskInfo_Source_BLOCK:
					buf.WriteString("BLOCK")
				case Resource_DiskInfo_Source_RAW:
					buf.WriteString("RAW")
				case Resource_DiskInfo_Source_PATH:
					buf.WriteString("PATH:")
					if p := s.GetPath(); p != nil {
						buf.WriteString(p.GetRoot())
					}
				case Resource_DiskInfo_Source_MOUNT:
					buf.WriteString("MOUNT:")
					if m := s.GetMount(); m != nil {
						buf.WriteString(m.GetRoot())
					}
				}
			}
			if p := d.GetPersistence(); p != nil {
				if d.GetSource() != nil {
					buf.WriteString(",")
				}
				buf.WriteString(p.GetID())
			}
			if v := d.GetVolume(); v != nil {
				buf.WriteString(":")
				vconfig := v.GetContainerPath()
				if h := v.GetHostPath(); h != "" {
					vconfig = h + ":" + vconfig
				}
				if m := v.Mode; m != nil {
					switch *m {
					case RO:
						vconfig += ":ro"
					case RW:
						vconfig += ":rw"
					default:
						panic("unrecognized volume mode: " + m.String())
					}
				}
				buf.WriteString(vconfig)
			}
			buf.WriteString("]")
		}
		buf.WriteString(":")
		switch r.GetType() {
		case SCALAR:
			buf.WriteString(strconv.FormatFloat(r.GetScalar().GetValue(), 'f', -1, 64))
		case RANGES:
			buf.WriteString("[")
			ranges := Ranges(r.GetRanges().GetRange())
			for j := range ranges {
				if j > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(strconv.FormatUint(ranges[j].Begin, 10))
				buf.WriteString("-")
				buf.WriteString(strconv.FormatUint(ranges[j].End, 10))
			}
			buf.WriteString("]")
		case SET:
			buf.WriteString("{")
			items := r.GetSet().GetItem()
			for j := range items {
				if j > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(items[j])
			}
			buf.WriteString("}")
		}
	}
	return buf.String()
}

func (left *Resource) Validate() error {
	if left.GetName() == "" {
		return resourceErrorTypeIllegalName.Generate(noReason)
	}
	if _, ok := Value_Type_name[int32(left.GetType())]; !ok {
		return resourceErrorTypeIllegalType.Generate(noReason)
	}
	switch left.GetType() {
	case SCALAR:
		if s := left.GetScalar(); s == nil || left.GetRanges() != nil || left.GetSet() != nil {
			return resourceErrorTypeIllegalScalar.Generate(noReason)
		} else if s.GetValue() < 0 {
			return resourceErrorTypeIllegalScalar.Generate("value < 0")
		}
	case RANGES:
		r := left.GetRanges()
		if left.GetScalar() != nil || r == nil || left.GetSet() != nil {
			return resourceErrorTypeIllegalRanges.Generate(noReason)
		}
		for i, rr := range r.GetRange() {
			// ensure that ranges are not inverted
			if rr.Begin > rr.End {
				return resourceErrorTypeIllegalRanges.Generate("begin > end")
			}
			// ensure that ranges don't overlap (but not necessarily squashed)
			for j := i + 1; j < len(r.GetRange()); j++ {
				r2 := r.GetRange()[j]
				if rr.Begin <= r2.Begin && r2.Begin <= rr.End {
					return resourceErrorTypeIllegalRanges.Generate("overlapping ranges")
				}
			}
		}
	case SET:
		s := left.GetSet()
		if left.GetScalar() != nil || left.GetRanges() != nil || s == nil {
			return resourceErrorTypeIllegalSet.Generate(noReason)
		}
		unique := make(map[string]struct{}, len(s.GetItem()))
		for _, x := range s.GetItem() {
			if _, found := unique[x]; found {
				return resourceErrorTypeIllegalSet.Generate("duplicated elements")
			}
			unique[x] = struct{}{}
		}
	default:
		return resourceErrorTypeUnsupportedType.Generate(noReason)
	}

	// check for disk resource
	if disk := left.GetDisk(); disk != nil {
		if left.GetName() != "disk" {
			return resourceErrorTypeIllegalDisk.Generate("DiskInfo should not be set for \"" + left.GetName() + "\" resource")
		}
		if s := disk.GetSource(); s != nil {
			switch s.GetType() {
			case Resource_DiskInfo_Source_PATH,
				Resource_DiskInfo_Source_MOUNT:
				// these only contain optional members
			case Resource_DiskInfo_Source_BLOCK,
				Resource_DiskInfo_Source_RAW:
				// TODO(jdef): update w/ validation once the format of BLOCK and RAW
				// disks is known.
			case Resource_DiskInfo_Source_UNKNOWN:
				return resourceErrorTypeIllegalDisk.Generate(fmt.Sprintf("unsupported DiskInfo.Source.Type in %q", s))
			}
		}
	}

	if rs := left.GetReservations(); len(rs) == 0 {
		// check for "pre-reservation-refinement" format
		if _, err := roles.Parse(left.GetRole()); err != nil {
			return resourceErrorTypeIllegalReservation.Generate(err.Error())
		}

		if r := left.GetReservation(); r != nil {
			if r.Type != nil {
				return resourceErrorTypeIllegalReservation.Generate(
					"Resource.ReservationInfo.type must not be set for the Resource.reservation field")
			}
			if r.Role != nil {
				return resourceErrorTypeIllegalReservation.Generate(
					"Resource.ReservationInfo.role must not be set for the Resource.reservation field")
			}
			// check for invalid state of (role,reservation) pair
			if left.GetRole() == "*" {
				return resourceErrorTypeIllegalReservation.Generate("default role cannot be dynamically reserved")
			}
		}
	} else {
		// check for "post-reservation-refinement" format
		for i := range rs {
			r := &rs[i]
			if r.Type == nil {
				return resourceErrorTypeIllegalReservation.Generate(
					"Resource.ReservationInfo.type must be set")
			}
			if r.Role == nil {
				return resourceErrorTypeIllegalReservation.Generate(
					"Resource.ReservationInfo.role must be set")
			}
			if _, err := roles.Parse(r.GetRole()); err != nil {
				return resourceErrorTypeIllegalReservation.Generate(err.Error())
			}
			if r.GetRole() == "*" {
				return resourceErrorTypeIllegalReservation.Generate(
					"role '*' cannot be reserved")
			}
		}
		// check that reservations are correctly refined
		ancestor := rs[0].GetRole()
		for i := 1; i < len(rs); i++ {
			r := &rs[i]
			if r.GetType() == Resource_ReservationInfo_STATIC {
				return resourceErrorTypeIllegalReservation.Generate(
					"a refined reservation cannot be STATIC")
			}
			child := r.GetRole()
			if !roles.IsStrictSubroleOf(child, ancestor) {
				return resourceErrorTypeIllegalReservation.Generate(fmt.Sprintf(
					"role %q is not a refinement of %q", child, ancestor))
			}
		}

		// Additionally, we allow the "pre-reservation-refinement" format to be set
		// as long as there is only one reservation, and the `Resource.role` and
		// `Resource.reservation` fields are consistent with the reservation.
		if len(rs) == 1 {
			if r := left.Role; r != nil && *r != rs[0].GetRole() {
				return resourceErrorTypeIllegalReservation.Generate(fmt.Sprintf(
					"'Resource.role' field with %q does not match the role %q in 'Resource.reservations'",
					*r, rs[0].GetRole()))
			}

			switch rs[0].GetType() {
			case Resource_ReservationInfo_STATIC:
				return resourceErrorTypeIllegalReservation.Generate(
					"'Resource.reservation' must not be set if the single reservation in 'Resource.reservations' is STATIC")
			case Resource_ReservationInfo_DYNAMIC:
				if (left.Role == nil) != (left.GetReservation() == nil) {
					return resourceErrorTypeIllegalReservation.Generate(
						"'Resource.role' and 'Resource.reservation' must both be set or both not be set if the single reservation in 'Resource.reservations' is DYNAMIC")
				}
				if r := left.GetReservation(); r != nil && r.GetPrincipal() != rs[0].GetPrincipal() {
					return resourceErrorTypeIllegalReservation.Generate(fmt.Sprintf(
						"'Resource.reservation.principal' with %q does not match the principal %q in 'Resource.reservations'",
						r.GetPrincipal(), rs[0].GetPrincipal()))
				}
				// TODO(jdef) come up with a better way to compare labels
				if r := left.GetReservation(); r != nil && !reflect.DeepEqual(r.GetLabels(), rs[0].GetLabels()) {
					return resourceErrorTypeIllegalReservation.Generate(fmt.Sprintf(
						"'Resource.reservation.labels' with %q does not match the labels %q in 'Resource.reservations'",
						r.GetLabels(), rs[0].GetLabels()))
				}
			case Resource_ReservationInfo_UNKNOWN:
				return resourceErrorTypeIllegalReservation.Generate("Unsupported 'Resource.ReservationInfo.type'")
			}
		} else {
			if r := left.Role; r != nil {
				return resourceErrorTypeIllegalReservation.Generate(
					"'Resource.role' must not be set if there is more than one reservation in 'Resource.reservations'")
			}
			if r := left.GetReservation(); r != nil {
				return resourceErrorTypeIllegalReservation.Generate(
					"'Resource.reservation' must not be set if there is more than one reservation in 'Resource.reservations'")
			}
		}
	}

	// Check that shareability is enabled for supported resource types.
	// For now, it is for persistent volumes only.
	// NOTE: We need to modify this once we extend shareability to other
	// resource types.
	if s := left.GetShared(); s != nil {
		if left.GetName() != "disk" {
			return resourceErrorTypeIllegalShare.Generate(fmt.Sprintf(
				"Resource %q cannot be shared", left.GetName()))
		}
		if p := left.GetDisk().GetPersistence(); p == nil {
			return resourceErrorTypeIllegalShare.Generate("only persistent volumes can be shared")
		}
	}

	return nil
}

func (r *Resource_ReservationInfo) Equivalent(right *Resource_ReservationInfo) bool {
	if (r == nil) != (right == nil) {
		return false
	}
	return r.GetPrincipal() == right.GetPrincipal()
}

func (left *Resource_DiskInfo) Equivalent(right *Resource_DiskInfo) bool {
	// NOTE: We ignore 'volume' inside DiskInfo when doing comparison
	// because it describes how this resource will be used which has
	// nothing to do with the Resource object itself. A framework can
	// use this resource and specify different 'volume' every time it
	// uses it.
	// see https://github.com/apache/mesos/blob/0.25.0/src/common/resources.cpp#L67
	if (left == nil) != (right == nil) {
		return false
	}

	if a, b := left.GetSource(), right.GetSource(); (a == nil) != (b == nil) {
		return false
	} else if a != nil {
		if a.GetType() != b.GetType() {
			return false
		}
		if aa, bb := a.GetMount(), b.GetMount(); (aa == nil) != (bb == nil) {
			return false
		} else if aa.GetRoot() != bb.GetRoot() {
			return false
		}
		if aa, bb := a.GetPath(), b.GetPath(); (aa == nil) != (bb == nil) {
			return false
		} else if aa.GetRoot() != bb.GetRoot() {
			return false
		}
		if aa, bb := a.GetID(), b.GetID(); aa != bb {
			return false
		}
		if aa, bb := a.GetProfile(), b.GetProfile(); aa != bb {
			return false
		}
		if aa, bb := a.GetMetadata(), b.GetMetadata(); (aa == nil) != (bb == nil) {
			return false
		} else if !reflect.DeepEqual(aa.GetLabels(), bb.GetLabels()) {
			// TODO(jdef) can we do better than DeepEqual here?
			return false
		}
	}

	if a, b := left.GetPersistence(), right.GetPersistence(); (a == nil) != (b == nil) {
		return false
	} else if a != nil {
		return a.GetID() == b.GetID()
	}

	return true
}

// Equivalent returns true if right is equivalent to left (differs from Equal in that
// deeply nested values are test for equivalence, not equality).
func (left *Resource) Equivalent(right Resource) bool {
	if left.GetName() != right.GetName() ||
		left.GetType() != right.GetType() ||
		left.GetRole() != right.GetRole() {
		return false
	}
	if !left.GetReservation().Equivalent(right.GetReservation()) {
		return false
	}
	if !left.GetDisk().Equivalent(right.GetDisk()) {
		return false
	}
	if (left.Revocable == nil) != (right.Revocable == nil) {
		return false
	}

	switch left.GetType() {
	case SCALAR:
		return left.GetScalar().Compare(right.GetScalar()) == 0
	case RANGES:
		return Ranges(left.GetRanges().GetRange()).Equivalent(right.GetRanges().GetRange())
	case SET:
		return left.GetSet().Compare(right.GetSet()) == 0
	default:
		return false
	}
}

// Addable tests if we can add two Resource objects together resulting in one
// valid Resource object. For example, two Resource objects with
// different name, type or role are not addable.
func (left *Resource) Addable(right Resource) bool {
	if left.GetName() != right.GetName() ||
		left.GetType() != right.GetType() ||
		left.GetRole() != right.GetRole() {
		return false
	}
	if !left.GetReservation().Equivalent(right.GetReservation()) {
		return false
	}
	if !left.GetDisk().Equivalent(right.GetDisk()) {
		return false
	}

	if ls := left.GetDisk().GetSource(); ls != nil {
		switch ls.GetType() {
		case Resource_DiskInfo_Source_PATH:
			// Two PATH resources can be added if their disks are identical
		case Resource_DiskInfo_Source_BLOCK,
			Resource_DiskInfo_Source_MOUNT:
			// Two resources that represent exclusive 'MOUNT' or 'RAW' disks
			// cannot be added together; this would defeat the exclusivity.
			return false
		case Resource_DiskInfo_Source_RAW:
			// We can only add resources representing 'RAW' disks if
			// they have no identity or are identical.
			if ls.GetID() != "" {
				return false
			}
		case Resource_DiskInfo_Source_UNKNOWN:
			panic("unreachable")
		}
	}

	// from apache/mesos: src/common/resources.cpp
	// TODO(jieyu): Even if two Resource objects with DiskInfo have the
	// same persistence ID, they cannot be added together. In fact, this
	// shouldn't happen if we do not add resources from different
	// namespaces (e.g., across slave). Consider adding a warning.
	if left.GetDisk().GetPersistence() != nil {
		return false
	}
	if (left.Revocable == nil) != (right.Revocable == nil) {
		return false
	}
	return true
}

// Subtractable tests if we can subtract "right" from "left" resulting in one
// valid Resource object. For example, two Resource objects with different
// name, type or role are not subtractable.
// NOTE: Set subtraction is always well defined, it does not require
// 'right' to be contained within 'left'. For example, assuming that
// "left = {1, 2}" and "right = {2, 3}", "left" and "right" are
// subtractable because "left - right = {1}". However, "left" does not
// contain "right".
func (left *Resource) Subtractable(right Resource) bool {
	if left.GetName() != right.GetName() ||
		left.GetType() != right.GetType() ||
		left.GetRole() != right.GetRole() {
		return false
	}
	if !left.GetReservation().Equivalent(right.GetReservation()) {
		return false
	}
	if !left.GetDisk().Equivalent(right.GetDisk()) {
		return false
	}

	if ls := left.GetDisk().GetSource(); ls != nil {
		switch ls.GetType() {
		case Resource_DiskInfo_Source_PATH:
			// Two PATH resources can be subtracted if their disks are identical
		case Resource_DiskInfo_Source_BLOCK,
			Resource_DiskInfo_Source_MOUNT:
			// Two resources that represent exclusive 'MOUNT' or 'RAW' disks
			// cannot be substracted from each other if they are not the same;
			// this would defeat the exclusivity.
			if !left.Equivalent(right) {
				return false
			}
		case Resource_DiskInfo_Source_RAW:
			// We can only add resources representing 'RAW' disks if
			// they have no identity or refer to the same disk.
			if ls.GetID() != "" && !left.Equivalent(right) {
				return false
			}
		case Resource_DiskInfo_Source_UNKNOWN:
			panic("unreachable")
		}
	}

	// NOTE: For Resource objects that have DiskInfo, we can only do
	// subtraction if they are **equal**.
	if left.GetDisk().GetPersistence() != nil && !left.Equivalent(right) {
		return false
	}
	if (left.Revocable == nil) != (right.Revocable == nil) {
		return false
	}
	return true
}

// Contains tests if "right" is contained in "left".
func (left Resource) Contains(right Resource) bool {
	if !left.Subtractable(right) {
		return false
	}
	switch left.GetType() {
	case SCALAR:
		return right.GetScalar().Compare(left.GetScalar()) <= 0
	case RANGES:
		return right.GetRanges().Compare(left.GetRanges()) <= 0
	case SET:
		return right.GetSet().Compare(left.GetSet()) <= 0
	default:
		return false
	}
}

// Subtract removes right from left.
// This func panics if the resource types don't match.
func (left *Resource) Subtract(right Resource) {
	switch right.checkType(left.GetType()) {
	case SCALAR:
		left.Scalar = left.GetScalar().Subtract(right.GetScalar())
	case RANGES:
		left.Ranges = left.GetRanges().Subtract(right.GetRanges())
	case SET:
		left.Set = left.GetSet().Subtract(right.GetSet())
	}
}

// Add adds right to left.
// This func panics if the resource types don't match.
func (left *Resource) Add(right Resource) {
	switch right.checkType(left.GetType()) {
	case SCALAR:
		left.Scalar = left.GetScalar().Add(right.GetScalar())
	case RANGES:
		left.Ranges = left.GetRanges().Add(right.GetRanges())
	case SET:
		left.Set = left.GetSet().Add(right.GetSet())
	}
}

// checkType panics if the type of this resources != t
func (left *Resource) checkType(t Value_Type) Value_Type {
	if left != nil && left.GetType() != t {
		panic(fmt.Sprintf("expected type %v instead of %v", t, left.GetType()))
	}
	return t
}

// IsEmpty returns true if the value of this resource is equivalent to the zero-value,
// where a zero-length slice or map is equivalent to a nil reference to such.
func (left *Resource) IsEmpty() bool {
	if left == nil {
		return true
	}
	switch left.GetType() {
	case SCALAR:
		return left.GetScalar().GetValue() == 0
	case RANGES:
		return len(left.GetRanges().GetRange()) == 0
	case SET:
		return len(left.GetSet().GetItem()) == 0
	}
	return false
}

// IsUnreserved returns true if this resource neither statically or dynamically reserved.
// A resource is considered statically reserved if it has a non-default role.
func (left *Resource) IsUnreserved() bool {
	// role != RoleDefault     -> static reservation
	// GetReservation() != nil -> dynamic reservation
	// return {no-static-reservation} && {no-dynamic-reservation}
	return (left.Role == nil || left.GetRole() == "*") && left.GetReservation() == nil && len(left.GetReservations()) == 0
}

// IsReserved returns true if this resource has been reserved for the given role.
// If role=="" then return true if there are no static or dynamic reservations for this resource.
// It's expected that this Resource has already been validated (see Validate).
func (left *Resource) IsReserved(role string) bool {
	return !left.IsUnreserved() && (role == "" || role == left.ReservationRole())
}

// ReservationRole returns the role for which the resource is reserved. Callers should check the
// reservation status of the resource via IsReserved prior to invoking this func.
func (r *Resource) ReservationRole() string {
	// if using reservation refinement, return the role of the last refinement
	rs := r.GetReservations()
	if x := len(rs); x > 0 {
		return rs[x-1].GetRole()
	}
	// if using the old reservation API, role is a first class field of Resource
	// (and it's never stored in Resource.Reservation).
	return r.GetRole()
}

// IsAllocatableTo returns true if the resource may be allocated to the given role.
func (left *Resource) IsAllocatableTo(role string) bool {
	if left.IsUnreserved() {
		return true
	}
	r := left.ReservationRole()
	return role == r || roles.IsStrictSubroleOf(role, r)
}

// IsDynamicallyReserved returns true if this resource has a non-nil reservation descriptor
func (left *Resource) IsDynamicallyReserved() bool {
	if left.IsReserved("") {
		if left.GetReservation() != nil {
			return true
		}
		rs := left.GetReservations()
		return rs[len(rs)-1].GetType() == Resource_ReservationInfo_DYNAMIC
	}
	return false
}

// IsRevocable returns true if this resource has a non-nil revocable descriptor
func (left *Resource) IsRevocable() bool {
	return left.GetRevocable() != nil
}

// IsPersistentVolume returns true if this is a disk resource with a non-nil Persistence descriptor
func (left *Resource) IsPersistentVolume() bool {
	return left.GetDisk().GetPersistence() != nil
}

// IsDisk returns true if this is a disk resource of the specified type.
func (left *Resource) IsDisk(t Resource_DiskInfo_Source_Type) bool {
	if s := left.GetDisk().GetSource(); s != nil {
		return s.GetType() == t
	}
	return false
}

// HasResourceProvider returns true if the given Resource object is provided by a resource provider.
func (left *Resource) HasResourceProvider() bool {
	return left.GetProviderID() != nil
}

// ToUnreserved returns a (cloned) view of the Resources w/o any reservation data. It does not modify
// the receiver.
func (rs Resources) ToUnreserved() (result Resources) {
	if rs == nil {
		return nil
	}
	for i := range rs {
		r := rs[i] // intentionally shallow-copy
		r.Reservations = nil
		r.Reservation = nil
		r.Role = nil
		result.Add1(r)
	}
	return
}
