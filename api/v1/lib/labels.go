package mesos

type labelList []Label // convenience type, for working with unwrapped Label slices

// Equivalent returns true if left and right have the same labels. Order is not important.
func (left *Labels) Equivalent(right *Labels) bool {
	return labelList(left.GetLabels()).Equivalent(labelList(right.GetLabels()))
}

// Equivalent returns true if left and right have the same labels. Order is not important.
func (left labelList) Equivalent(right labelList) bool {
	if len(left) != len(right) {
		return false
	} else {
		for i := range left {
			found := false
			for j := range right {
				if left[i].Equivalent(right[j]) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
}

// Equivalent returns true if left and right represent the same Label.
func (left Label) Equivalent(right Label) bool {
	if left.Key != right.Key {
		return false
	}
	if left.Value == nil {
		return right.Value == nil
	} else {
		return right.Value != nil && *left.Value == *right.Value
	}
}
