package mesos

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/gogo/protobuf/proto"
)

type (
	Role              string
	Resources         []Resource
	ResourceFilter    func(*Resource) bool
	ResourceFilters   []ResourceFilter
	ResourceErrorType int

	ResourceError struct {
		errorType ResourceErrorType
		reason    string
		spec      Resource
	}

	// functional option for resource flattening, via Flatten
	FlattenOpt func(*FlattenConfig)

	FlattenConfig struct {
		Role        string
		Reservation *Resource_ReservationInfo
	}
)

const (
	RoleDefault = Role("*")

	ResourceErrorTypeIllegalName ResourceErrorType = iota
	ResourceErrorTypeIllegalType
	ResourceErrorTypeUnsupportedType
	ResourceErrorTypeIllegalScalar
	ResourceErrorTypeIllegalRanges
	ResourceErrorTypeIllegalSet
	ResourceErrorTypeIllegalDisk
	ResourceErrorTypeIllegalReservation

	noReason = "" // make error generation code more readable
)

var (
	AnyResources = ResourceFilter(func(r *Resource) bool {
		return r != nil && !r.IsEmpty()
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
	RangeResources = ResourceFilter(func(r *Resource) bool {
		return r.GetType() == RANGES
	})
	SetResources = ResourceFilter(func(r *Resource) bool {
		return r.GetType() == SET
	})

	resourceErrorMessages = map[ResourceErrorType]string{
		ResourceErrorTypeIllegalName:        "missing or illegal resource name",
		ResourceErrorTypeIllegalType:        "missing or illegal resource type",
		ResourceErrorTypeUnsupportedType:    "unsupported resource type",
		ResourceErrorTypeIllegalScalar:      "illegal scalar resource",
		ResourceErrorTypeIllegalRanges:      "illegal ranges resource",
		ResourceErrorTypeIllegalSet:         "illegal set resource",
		ResourceErrorTypeIllegalDisk:        "illegal disk resource",
		ResourceErrorTypeIllegalReservation: "illegal resource reservation",
	}
)

func (t ResourceErrorType) Generate(reason string) error {
	msg := resourceErrorMessages[t]
	if reason != noReason {
		if msg != "" {
			msg += ": " + reason
		} else {
			msg = reason
		}
	}
	return &ResourceError{errorType: t, reason: msg}
}

func (err *ResourceError) Type() ResourceErrorType { return err.errorType }
func (err *ResourceError) Reason() string          { return err.reason }
func (err *ResourceError) Resource() Resource      { return err.spec }

func (err *ResourceError) Error() string {
	if err.reason != "" {
		return "resource error: " + err.reason
	}
	return "resource error"
}

func (r Role) IsDefault() bool {
	return r == RoleDefault
}

func (r Role) Assign() FlattenOpt {
	return func(fc *FlattenConfig) {
		fc.Role = string(r)
	}
}

func (r Role) Proto() *string {
	s := string(r)
	return &s
}

func (rf ResourceFilter) Or(f ResourceFilter) ResourceFilter {
	return ResourceFilter(func(r *Resource) bool {
		return rf(r) || f(r)
	})
}

func (rf ResourceFilter) And(f ResourceFilter) ResourceFilter {
	return ResourceFilters{rf, f}.Predicate()
}

func (rf ResourceFilter) Select(resources Resources) (result Resources) {
	for i := range resources {
		if rf(&resources[i]) {
			result.add(resources[i])
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

func (resources Resources) CPUs() (float64, bool) {
	v := resources.SumScalars(NamedResources("cpus"))
	if v != nil {
		return v.Value, true
	}
	return 0, false
}

func (resources Resources) Memory() (uint64, bool) {
	v := resources.SumScalars(NamedResources("mem"))
	if v != nil {
		return uint64(v.Value), true
	}
	return 0, false
}

func (resources Resources) Disk() (uint64, bool) {
	v := resources.SumScalars(NamedResources("disk"))
	if v != nil {
		return uint64(v.Value), true
	}
	return 0, false
}

func (resources Resources) Ports() (Ranges, bool) {
	v := resources.SumRanges(NamedResources("ports"))
	if v != nil {
		return Ranges(v.Range), true
	}
	return nil, false
}

func (resources Resources) SumScalars(rf ResourceFilter) *Value_Scalar {
	predicate := ResourceFilters{rf, ScalarResources}.Predicate()
	var x *Value_Scalar
	for i := range resources {
		if !predicate(&resources[i]) {
			continue
		}
		x = x.Add(resources[i].GetScalar())
	}
	return x
}

func (resources Resources) SumRanges(rf ResourceFilter) *Value_Ranges {
	predicate := ResourceFilters{rf, RangeResources}.Predicate()
	var x *Value_Ranges
	for i := range resources {
		if !predicate(&resources[i]) {
			continue
		}
		x = x.Add(resources[i].GetRanges())
	}
	return x
}

func (resources Resources) SumSets(rf ResourceFilter) *Value_Set {
	predicate := ResourceFilters{rf, SetResources}.Predicate()
	var x *Value_Set
	for i := range resources {
		if !predicate(&resources[i]) {
			continue
		}
		x = x.Add(resources[i].GetSet())
	}
	return x
}

func (resources Resources) Types() map[string]Value_Type {
	m := map[string]Value_Type{}
	for i := range resources {
		m[resources[i].GetName()] = resources[i].GetType()
	}
	return m
}

func (resources Resources) Names() (names []string) {
	m := map[string]struct{}{}
	for i := range resources {
		n := resources[i].GetName()
		if _, ok := m[n]; !ok {
			m[n] = struct{}{}
			names = append(names, n)
		}
	}
	return
}

func (resources Resources) sameTotals(result Resources) bool {
	// from: https://github.com/apache/mesos/blob/master/src/common/resources.cpp
	// This is a sanity check to ensure the amount of each type of
	// resource does not change.
	// TODO(jieyu): Currently, we only check known resource types like
	// cpus, mem, disk, ports, etc. We should generalize this.
	var (
		c1, c2 = result.CPUs()
		m1, m2 = result.Memory()
		d1, d2 = result.Disk()
		p1, p2 = result.Ports()

		c3, c4 = resources.CPUs()
		m3, m4 = resources.Memory()
		d3, d4 = resources.Disk()
		p3, p4 = resources.Ports()
	)
	return c1 == c3 && c2 == c4 &&
		m1 == m3 && m2 == m4 &&
		d1 == d3 && d2 == d4 &&
		p1.Equivalent(p3) && p2 == p4
}

func (resources Resources) Find(targets Resources) (total Resources) {
	for i := range targets {
		found := resources.find(targets[i])

		// each target *must* be found
		if len(found) == 0 {
			return nil
		}

		total.Add(found...)
	}
	return total
}

func (resources Resources) find(target Resource) Resources {
	var (
		total      = resources.Clone()
		remaining  = Resources{target}.Flatten()
		found      Resources
		predicates = ResourceFilters{
			ReservedResources(target.GetRole()),
			UnreservedResources,
			AnyResources,
		}
	)
	for _, predicate := range predicates {
		filtered := predicate.Select(total)
		for i := range filtered {
			// need to flatten to ignore the roles in ContainsAll()
			flattened := Resources{filtered[i]}.Flatten()
			if flattened.ContainsAll(remaining) {
				// target has been found, return the result
				return found.Add(remaining.Flatten(
					Role(filtered[i].GetRole()).Assign(),
					filtered[i].Reservation.Assign())...)
			}
			if remaining.ContainsAll(flattened) {
				found.add(filtered[i])
				total.subtract(filtered[i])
				remaining.Subtract(flattened...)
				break
			}
		}
	}
	return nil
}

func (ri *Resource_ReservationInfo) Assign() FlattenOpt {
	return func(fc *FlattenConfig) {
		fc.Reservation = ri
	}
}

func (resources Resources) Flatten(opts ...FlattenOpt) (flattened Resources) {
	fc := &FlattenConfig{}
	for _, f := range opts {
		f(fc)
	}
	if fc.Role == "" {
		fc.Role = string(RoleDefault)
	}
	// we intentionally manipulate a copy 'r' of the item in resources
	for _, r := range resources {
		r.Role = &fc.Role
		r.Reservation = fc.Reservation
		flattened.add(r)
	}
	return
}

func (resources Resources) Validate() error {
	for i := range resources {
		err := resources[i].Validate()
		if err != nil {
			// augment ResourceError's with the resource that failed to validate
			if resourceError, ok := err.(*ResourceError); ok {
				r := proto.Clone(&resources[i]).(*Resource)
				resourceError.spec = *r
			}
			return err
		}
	}
	return nil
}

func (resources Resources) contains(that Resource) bool {
	for i := range resources {
		if resources[i].Contains(that) {
			return true
		}
	}
	return false
}

func (resources Resources) Equivalent(that Resources) bool {
	return resources.ContainsAll(that) && that.ContainsAll(resources)
}

func (resources Resources) Contains(that Resource) bool {
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
	for i := range resources {
		rr := proto.Clone(&resources[i]).(*Resource)
		clone = append(clone, *rr)
	}
	return clone
}

// ContainsAll returns true if this set of Resources contains that set of (presumably pre-validated) Resources.
func (resources Resources) ContainsAll(that Resources) bool {
	remaining := resources.Clone()
	for i := range that {
		// NOTE: We use contains() because Resources only contain valid
		// Resource objects, and we don't want the performance hit of the
		// validity check.
		if !remaining.contains(that[i]) {
			return false
		}
		remaining.subtract(that[i])
	}
	return true
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
				resources.subtract(that[i])
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

// add adds `that` to the receiving `resources` and returns the result (the modified
// `resources` receiver).
func (resources *Resources) add(that Resource) (rs Resources) {
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

// minus calculates and returns the result of `resources - that` without modifying either
// the receiving `resources` or `that`.
func (resources *Resources) minus(that Resource) Resources {
	x := resources.Clone()
	return x.subtract(that)
}

// subtract subtracts `that` from the receiving `resources` and returns the result (the modified
// `resources` receiver).
func (resources *Resources) subtract(that Resource) Resources {
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
			if p := d.GetPersistence(); p != nil {
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
		return ResourceErrorTypeIllegalName.Generate(noReason)
	}
	if _, ok := Value_Type_name[int32(left.GetType())]; !ok {
		return ResourceErrorTypeIllegalType.Generate(noReason)
	}
	switch left.GetType() {
	case SCALAR:
		if s := left.GetScalar(); s == nil || left.GetRanges() != nil || left.GetSet() != nil {
			return ResourceErrorTypeIllegalScalar.Generate(noReason)
		} else if s.GetValue() < 0 {
			return ResourceErrorTypeIllegalScalar.Generate("value < 0")
		}
	case RANGES:
		if r := left.GetRanges(); left.GetScalar() != nil || r == nil || left.GetSet() != nil {
			return ResourceErrorTypeIllegalRanges.Generate(noReason)
		} else {
			for i, rr := range r.GetRange() {
				// ensure that ranges are not inverted
				if rr.Begin > rr.End {
					return ResourceErrorTypeIllegalRanges.Generate("begin > end")
				}
				// ensure that ranges don't overlap (but not necessarily squashed)
				for j := i + 1; j < len(r.GetRange()); j++ {
					r2 := r.GetRange()[j]
					if rr.Begin <= r2.Begin && r2.Begin <= rr.End {
						return ResourceErrorTypeIllegalRanges.Generate("overlapping ranges")
					}
				}
			}
		}
	case SET:
		if s := left.GetSet(); left.GetScalar() != nil || left.GetRanges() != nil || s == nil {
			return ResourceErrorTypeIllegalSet.Generate(noReason)
		} else {
			unique := make(map[string]struct{}, len(s.GetItem()))
			for _, x := range s.GetItem() {
				if _, found := unique[x]; found {
					return ResourceErrorTypeIllegalSet.Generate("duplicated elements")
				}
				unique[x] = struct{}{}
			}
		}
	default:
		return ResourceErrorTypeUnsupportedType.Generate(noReason)
	}

	// check for disk resource
	if left.GetDisk() != nil && left.GetName() != "disk" {
		return ResourceErrorTypeIllegalDisk.Generate("DiskInfo should not be set for \"" + left.GetName() + "\" resource")
	}

	// check for invalid state of (role,reservation) pair
	if left.GetRole() == string(RoleDefault) && left.GetReservation() != nil {
		return ResourceErrorTypeIllegalReservation.Generate("default role cannot be dynamically assigned")
	}

	return nil
}

func (left *Resource_ReservationInfo) Equivalent(right *Resource_ReservationInfo) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	return left.GetPrincipal() == right.GetPrincipal()
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
	return left.GetRole() == string(RoleDefault) && left.GetReservation() == nil
}

// IsReserved returns true if this resource has been reserved for the given role.
// If role=="" then return true if there are no static or dynamic reservations for this resource.
// It's expected that this Resource has already been validated (see Validate).
func (left *Resource) IsReserved(role string) bool {
	if role != "" {
		return !left.IsUnreserved() && role == left.GetRole()
	} else {
		return !left.IsUnreserved()
	}
}

// IsDynamicallyReserved returns true if this resource has a non-nil reservation descriptor
func (left *Resource) IsDynamicallyReserved() bool {
	return left.GetReservation() != nil
}

// IsRevocable returns true if this resource has a non-nil revocable descriptor
func (left *Resource) IsRevocable() bool {
	return left.GetRevocable() != nil
}

// IsPersistentVolume returns true if this is a disk resource with a non-nil Persistence descriptor
func (left *Resource) IsPersistentVolume() bool {
	return left.GetDisk().GetPersistence() != nil
}
