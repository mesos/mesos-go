package mesos

import (
	"strconv"
	"testing"
)

func TestEquivalent_Labels(t *testing.T) {
	for ti, tc := range []struct {
		l1, l2 *Labels
		wants  bool
	}{
		{wants: true},
		{l1: &Labels{}, wants: true},
		{l2: &Labels{}, wants: true},
		{l1: &Labels{}, l2: &Labels{}, wants: true},
		{l1: &Labels{}, l2: &Labels{Labels: []Label{}}, wants: true},
		{l2: &Labels{Labels: []Label{}}, wants: true},
		{l1: &Labels{Labels: []Label{}}, l2: &Labels{Labels: []Label{}}, wants: true},
		{
			l1: &Labels{Labels: []Label{
				{Key: "a"},
			}},
			l2: &Labels{Labels: []Label{
				{Key: "a"},
			}},
			wants: true,
		},
		{
			l1: &Labels{Labels: []Label{
				{Key: "c"},
				{Key: "b"},
				{Key: "a"},
			}},
			l2: &Labels{Labels: []Label{
				{Key: "a"},
				{Key: "b"},
				{Key: "c"},
			}},
			wants: true,
		},
		{
			l1: &Labels{Labels: []Label{
				{Key: "a"},
			}},
			l2: &Labels{Labels: []Label{
				{Key: "a"},
				{Key: "b"},
				{Key: "c"},
			}},
		},
		{
			l1: &Labels{Labels: []Label{
				{Key: "a"},
			}},
			l2: &Labels{Labels: []Label{
				{Key: "c"},
			}},
		},
		{
			l1: &Labels{Labels: []Label{
				{Key: "a"},
			}},
			l2: &Labels{Labels: []Label{}},
		},
		{
			l1: &Labels{Labels: []Label{
				{Key: "a"},
			}},
		},
	} {
		t.Run(strconv.Itoa(ti), func(t *testing.T) {
			eq := tc.l1.Equivalent(tc.l2)
			if eq != tc.wants {
				if tc.wants {
					t.Fatal("expected equivalent labels")
				} else {
					t.Fatal("expected non-equivalent labels")
				}
			}
		})
	}
}
