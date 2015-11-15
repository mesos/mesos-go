package mesos

import (
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
)

const (
	RoleDefault = "*"
)

type (
	Resources       []*Resource
	ResourceFilter  func(*Resource) bool
	ResourceFilters []ResourceFilter
)

var (
	AnyResources = ResourceFilter(func(r *Resource) bool {
		return r != nil
	})

	UnreservedResources = ResourceFilter(func(r *Resource) bool {
		return r.IsUnreserved()
	})

	PersistentVolumes = ResourceFilter(func(r *Resource) bool {
		return r.IsPersistentVolume()
	})

	RevocableResources = ResourceFilter(func(r *Resource) bool {
		return r.IsRevocable()
	})

	ScalarResources = ResourceFilter(func(r *Resource) bool {
		return r.GetType() == SCALAR
	})
)

func (rf ResourceFilter) Or(f ResourceFilter) ResourceFilter {
	return ResourceFilter(func(r *Resource) bool {
		return rf(r) || f(r)
	})
}

func (rf ResourceFilter) Apply(resources Resources) (result Resources) {
	for _, r := range resources {
		if rf(r) {
			result.Add(r)
		}
	}
	return
}

func (rf ResourceFilters) Predicate() ResourceFilter {
	return ResourceFilter(func(r *Resource) bool {
		for _, f := range rf {
			if !f(r) {
				return false
			}
		}
		return true
	})
}

func ReservedResources(role string) ResourceFilter {
	return ResourceFilter(func(r *Resource) bool {
		return r.IsReserved(role)
	})
}

func NamedResources(name string) ResourceFilter {
	return ResourceFilter(func(r *Resource) bool {
		return r.GetName() == name
	})
}

func (resources Resources) Apply(operation *Offer_Operation) (Resources, error) {
	result := resources.Clone()
	switch operation.GetType() {
	case LAUNCH:
		// launch op doens't alter offer resources
	case RESERVE:
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
	case UNRESERVE:
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
	case CREATE:
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
			stripped := proto.Clone(v).(*Resource)
			stripped.Disk = nil
			if !result.Contains(stripped) {
				return nil, errors.New("invalid CREATE operation: insufficient disk resources")
			}

			result.Subtract(stripped)
			result.Add(v)
		}
	case DESTROY:
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
			stripped := proto.Clone(v).(*Resource)
			stripped.Disk = nil
			result.Subtract(v)
			result.Add(stripped)
		}
	default:
		return nil, errors.New("unknown offer operation: " + operation.GetType().String())
	}
	//TODO(jdef) add equiv of sanity CHECK here
	return result, nil
}

func (resources Resources) Find(targets Resources) (total Resources) {
	for _, target := range targets {
		found := resources.find(target)

		// each target *must* be found
		if len(found) == 0 {
			return nil
		}

		total.AddAll(found)
	}
	return total
}

func (resources Resources) find(target *Resource) Resources {
	var (
		total      = resources.Clone()
		remaining  = Resources{target}.Flatten("", nil)
		found      Resources
		predicates = ResourceFilters{
			ReservedResources(target.GetRole()),
			UnreservedResources,
			AnyResources,
		}
	)
	for _, predicate := range predicates {
		for _, r := range predicate.Apply(total) {
			// need to flatten to ignore the roles in ContainsAll()
			flattened := Resources{r}.Flatten("", nil)
			if flattened.ContainsAll(remaining) {
				// target has been found, return the result
				found.AddAll(remaining.Flatten(r.GetRole(), r.GetReservation()))
				return found
			}
			if remaining.ContainsAll(flattened) {
				found.Add(r)
				total.Subtract(r)
				remaining.SubtractAll(flattened)
				break
			}
		}
	}
	return nil
}

func (resources Resources) Flatten(role string, ri *Resource_ReservationInfo) (flattened Resources) {
	if role == "" {
		role = RoleDefault
	}
	for _, r := range resources {
		if r == nil {
			continue
		}
		r.Role = &role
		if ri == nil {
			r.Reservation = nil
		} else {
			r.Reservation = ri
		}
		flattened.Add(r)
	}
	return
}

func (resources Resources) Validate() error {
	for _, r := range resources {
		err := r.Validate()
		if err != nil {
			return fmt.Errorf("resource %v is invalid: %v", r, err)
		}
	}
	return nil
}

func (resources Resources) contains(that *Resource) bool {
	for _, r := range resources {
		if r.Contains(that) {
			return true
		}
	}
	return false
}

func (resources Resources) Contains(that *Resource) bool {
	// NOTE: We must validate 'that' because invalid resources can lead
	// to false positives here (e.g., "cpus:-1" will return true). This
	// is because 'contains' assumes resources are valid.
	return that.Validate() == nil && resources.contains(that)
}

func (resources Resources) Clone() Resources {
	if resources == nil {
		return nil
	}
	clone := make(Resources, 0, len(resources))
	for _, r := range resources {
		clone = append(clone, proto.Clone(r).(*Resource))
	}
	return clone
}

func (resources Resources) ContainsAll(that Resources) bool {
	remaining := resources.Clone()
	for _, r := range that {
		if !remaining.contains(r) {
			return false
		}
		remaining.Subtract(r)
	}
	return true
}

func (resources *Resources) SubtractAll(other Resources) {
	for _, r := range other {
		resources.Subtract(r)
	}
}

func (resources *Resources) AddAll(that Resources) {
	for _, r := range that {
		resources.Add(r)
	}
}

func (resources *Resources) Add(that *Resource) {
	if that.Validate() != nil || that.IsEmpty() {
		return
	}
	for _, r := range *resources {
		if r.Addable(that) {
			r.Add(that)
			return
		}
	}
	// cannot be combined with an existing resource
	*resources = append(*resources, proto.Clone(that).(*Resource))
}

func (resources *Resources) Subtract(that *Resource) {
	if that.Validate() != nil || that.IsEmpty() {
		return
	}
	for i, r := range *resources {
		if r.Subtractable(that) {
			r.Subtract(that)
			// remove the resource if it becomes invalid or zero.
			// need to do validation in order to strip negative scalar
			// resource objects.
			if r.Validate() != nil || r.IsEmpty() {
				// delete resource at i, without leaking an uncollectable *Resource
				// a, a[len(a)-1] = append(a[:i], a[i+1:]...), nil
				*resources, (*resources)[len(*resources)-1] = append((*resources)[:i], (*resources)[i+1:]...), nil
			}
			return
		}
	}
}

func (left *Resource) Validate() error {
	if left.GetName() == "" {
		return errors.New("empty resource name")
	}
	if _, ok := Value_Type_name[int32(left.GetType())]; !ok {
		return errors.New("invalid resource type")
	}
	switch left.GetType() {
	case SCALAR:
		if s := left.GetScalar(); s == nil || left.GetRanges() != nil || left.GetSet() != nil {
			return errors.New("invalid scalar resource")
		} else if s.GetValue() < 0 {
			return errors.New("invalid scalar resource: value < 0")
		}
	case RANGES:
		if r := left.GetRanges(); left.GetScalar() != nil || r == nil || left.GetSet() != nil {
			return errors.New("invalid ranges resource")
		} else {
			for i, rr := range r.GetRange() {
				// ensure that ranges are not inverted
				if rr.Begin > rr.End {
					return errors.New("invalid ranges resource: begin > end")
				}
				// ensure that ranges don't overlap (but not necessarily squashed)
				for j := i + 1; j < len(r.GetRange()); j++ {
					r2 := r.GetRange()[j]
					if rr.Begin <= r2.Begin && r2.Begin <= rr.End {
						return errors.New("invalid ranges resource: overlapping ranges")
					}
				}
			}
		}
	case SET:
		if s := left.GetSet(); left.GetScalar() != nil || left.GetRanges() != nil || s == nil {
			return errors.New("invalid set resource")
		} else {
			unique := make(map[string]struct{}, len(s.GetItem()))
			for _, x := range s.GetItem() {
				if _, found := unique[x]; found {
					return errors.New("invalid set resource: duplicated elements")
				}
				unique[x] = struct{}{}
			}
		}
	default:
		return errors.New("unsupported resource type")
	}

	// check for disk resource
	if left.GetDisk() != nil && left.GetName() != "disk" {
		return errors.New("DiskInfo should not be set for \"" + left.GetName() + "\" resource")
	}

	// check for invalid state of (role,reservation) pair
	if left.GetRole() == RoleDefault && left.GetReservation() != nil {
		return errors.New("invalid reservation: role \"" + RoleDefault + "\" cannot be dynamically assigned")
	}

	return nil
}

// Addable tests if we can add two Resource objects together resulting in one
// valid Resource object. For example, two Resource objects with
// different name, type or role are not addable.
func (left *Resource) Addable(right *Resource) bool {
	if left.GetName() != right.GetName() ||
		left.GetType() != right.GetType() ||
		left.GetRole() != right.GetRole() {
		return false
	}

	if !left.GetReservation().Equal(right.GetReservation()) {
		return false
	}

	if !left.GetDisk().Equal(right.GetDisk()) {
		return false
	}

	// from apache/mesos: src/common/resources.cpp
	// TODO(jieyu): Even if two Resource objects with DiskInfo have the
	// same persistence ID, they cannot be added together. In fact, this
	// shouldn't happen if we do not add resources from different
	// namespaces (e.g., across slave). Consider adding a warning.
	if left.GetDisk().GetPersistence() != nil {
		return false
	}

	if !left.GetRevocable().Equal(right.GetRevocable()) {
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
func (left *Resource) Subtractable(right *Resource) bool {
	if left.GetName() != right.GetName() ||
		left.GetType() != right.GetType() ||
		left.GetRole() != right.GetRole() {
		return false
	}

	if !left.GetReservation().Equal(right.GetReservation()) {
		return false
	}

	if !left.GetDisk().Equal(right.GetDisk()) {
		return false
	}

	// NOTE: For Resource objects that have DiskInfo, we can only do
	// subtraction if they are equal.
	if left.GetDisk().GetPersistence() != nil && !left.Equal(right) {
		return false
	}

	if !left.GetRevocable().Equal(right.GetRevocable()) {
		return false
	}

	return true
}

// Contains tests if "right" is contained in "left".
func (left *Resource) Contains(right *Resource) bool {
	if !left.Subtractable(right) {
		return false
	}
	switch left.GetType() {
	case SCALAR:
		return left.GetScalar().Compare(right.GetScalar()) < 1
	case RANGES:
		return left.GetRanges().Compare(right.GetRanges()) < 1
	case SET:
		return left.GetSet().Compare(right.GetSet()) < 1
	default:
		return false
	}
}

func (left *Value_Scalar) Compare(right *Value_Scalar) int {
	if left == nil {
		if right == nil {
			return 0
		}
		return -1
	} else if right == nil {
		return 1
	} else if left.Value < right.Value {
		return -1
	} else if left.Value > right.Value {
		return 1
	}
	return 0
}

func (_left *Value_Ranges) Compare(_right *Value_Ranges) int {
	var (
		left  = Ranges(_left.GetRange()).Squash()
		right = Ranges(_right.GetRange()).Squash()
	)
	if (&Value_Ranges{Range: left}).Equal(&Value_Ranges{Range: right}) {
		return 0
	}
	for _, a := range left {
		// make sure that this range is a subset of a range in right
		matched := false
		for _, b := range right {
			if a.Begin >= b.Begin && a.End <= b.End {
				matched = true
				break
			}
		}
		if !matched {
			return 1
		}
	}
	return -1
}

func (left *Value_Set) Compare(right *Value_Set) int {
	i, j := left.GetItem(), right.GetItem()
	if len(i) <= len(j) {
		b := make(map[string]struct{}, len(j))
		for _, x := range j {
			b[x] = struct{}{}
		}
		// make sure that each item on the left exists on the right,
		// otherwise left is not a subset of right.
		a := make(map[string]struct{}, len(i))
		for _, x := range i {
			if _, ok := b[x]; !ok {
				return 1
			}
			a[x] = struct{}{}
		}
		// if every item on the right also exists on the left, then
		// the sets are equal, otherwise left < right
		for x := range b {
			if _, ok := a[x]; !ok {
				return -1
			}
		}
		return 0
	}
	return 1
}

// Subtract removes right from left
func (left *Resource) Subtract(right *Resource) {
	switch left.GetType() {
	case SCALAR:
		if right != nil && right.GetType() != SCALAR {
			panic("right resource is not SCALAR")
		}
		x := left.GetScalar().GetValue() - right.GetScalar().GetValue()
		left.Scalar = &Value_Scalar{Value: x}
	case RANGES:
		if right != nil && right.GetType() != RANGES {
			panic("right resource is not RANGES")
		}
		x := Ranges(left.GetRanges().GetRange()).Squash()
		for _, r := range right.GetRanges().GetRange() {
			x = x.Remove(r)
		}
		left.Ranges = &Value_Ranges{Range: x}
	case SET:
		if right != nil && right.GetType() != SET {
			panic("right resource is not SET")
		}
		// for each item in right, remove it from left
		lefty := left.GetSet().GetItem()
		if len(lefty) == 0 {
			return
		}
		a := make(map[string]struct{}, len(lefty))
		for _, x := range lefty {
			a[x] = struct{}{}
		}
		for _, x := range right.GetSet().GetItem() {
			delete(a, x)
		}
		i := 0
		for k := range a {
			lefty[i] = k
			i++
		}
		left.Set = &Value_Set{Item: lefty[:len(a)]}
	}
}

func (left *Resource) Add(right *Resource) {
	switch left.GetType() {
	case SCALAR:
		if right != nil && right.GetType() != SCALAR {
			panic("right resource is not SCALAR")
		}
		x := left.GetScalar().GetValue() + right.GetScalar().GetValue()
		left.Scalar = &Value_Scalar{Value: x}
	case RANGES:
		if right != nil && right.GetType() != RANGES {
			panic("right resource is not RANGES")
		}
		x := Ranges(append(left.GetRanges().GetRange(), right.GetRanges().GetRange()...)).Squash()
		left.Ranges = &Value_Ranges{Range: x}
	case SET:
		if right != nil && right.GetType() != SET {
			panic("right resource is not SET")
		}
		lefty := left.GetSet().GetItem()
		righty := right.GetSet().GetItem()
		m := make(map[string]struct{}, len(lefty)+len(righty))
		for _, v := range lefty {
			m[v] = struct{}{}
		}
		for _, v := range righty {
			m[v] = struct{}{}
		}
		x := make([]string, 0, len(m))
		for v := range m {
			x = append(x, v)
		}
		left.Set = &Value_Set{Item: x}
	}
}

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

func (left *Resource) IsUnreserved() bool {
	return left.GetRole() == RoleDefault && left.GetReservation() == nil
}

func (left *Resource) IsReserved(role string) bool {
	if role != "" {
		return !left.IsUnreserved() && role == left.GetRole()
	} else {
		return !left.IsUnreserved()
	}
}

func (left *Resource) IsDynamicallyReserved() bool {
	return left.GetReservation() != nil
}

func (left *Resource) IsRevocable() bool {
	return left.GetRevocable() != nil
}

func (left *Resource) IsPersistentVolume() bool {
	return left.GetDisk().GetPersistence() != nil
}
