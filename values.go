package mesos

import (
	"sort"
)

func (left *Value_Scalar) Compare(right *Value_Scalar) int {
	if left == nil {
		if right.GetValue() == 0 {
			return 0
		}
		return -1
	} else if v := right.GetValue(); left.Value < v {
		return -1
	} else if left.Value > v {
		return 1
	}
	return 0
}

func (left *Value_Ranges) Compare(right *Value_Ranges) int {
	return Ranges(left.GetRange()).Compare(right.GetRange())
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

func (left *Value_Set) Add(right *Value_Set) *Value_Set {
	lefty := left.GetItem()
	righty := right.GetItem()
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
	return &Value_Set{Item: x}
}

func (left *Value_Set) Subtract(right *Value_Set) *Value_Set {
	// for each item in right, remove it from left
	lefty := left.GetItem()
	if len(lefty) == 0 {
		return &Value_Set{}
	}
	a := make(map[string]struct{}, len(lefty))
	for _, x := range lefty {
		a[x] = struct{}{}
	}
	for _, x := range right.GetItem() {
		delete(a, x)
	}
	i := 0
	for k := range a {
		lefty[i] = k
		i++
	}
	return &Value_Set{Item: lefty[:len(a)]}
}

func (left *Value_Ranges) Add(right *Value_Ranges) *Value_Ranges {
	a, b := Ranges(left.GetRange()), Ranges(right.GetRange())
	c := len(a) + len(b)
	if c == 0 {
		return nil
	}
	x := make(Ranges, c)
	if len(a) > 0 {
		copy(x, a)
	}
	if len(b) > 0 {
		copy(x[len(a):], b)
	}
	sort.Sort(x)
	return &Value_Ranges{
		Range: x.Squash(),
	}
}

func (left *Value_Ranges) Subtract(right *Value_Ranges) *Value_Ranges {
	a, b := Ranges(left.GetRange()), Ranges(right.GetRange())
	if len(a) > 1 {
		x := make(Ranges, len(a))
		copy(x, a)
		sort.Sort(x)
		a = x.Squash()
	}
	for _, r := range b {
		a = a.Remove(r)
	}
	if len(a) == 0 {
		return nil
	}
	return &Value_Ranges{Range: a}
}

func (left *Value_Scalar) Add(right *Value_Scalar) *Value_Scalar {
	return &Value_Scalar{Value: left.GetValue() + right.GetValue()}
}

func (left *Value_Scalar) Subtract(right *Value_Scalar) *Value_Scalar {
	return &Value_Scalar{Value: left.GetValue() - right.GetValue()}
}
