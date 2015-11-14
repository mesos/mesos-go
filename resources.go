package mesos

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
