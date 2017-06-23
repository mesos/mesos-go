package resources

import (
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/resourcefilters"
)

func SumScalars(rf resourcefilters.Filter, resources ...mesos.Resource) *mesos.Value_Scalar {
	predicate := resourcefilters.Filters{rf, resourcefilters.Scalar}
	var x *mesos.Value_Scalar
	for i := range resources {
		if !predicate.Accepts(&resources[i]) {
			continue
		}
		x = x.Add(resources[i].GetScalar())
	}
	return x
}

func SumRanges(rf resourcefilters.Filter, resources ...mesos.Resource) *mesos.Value_Ranges {
	predicate := resourcefilters.Filters{rf, resourcefilters.Range}
	var x *mesos.Value_Ranges
	for i := range resources {
		if !predicate.Accepts(&resources[i]) {
			continue
		}
		x = x.Add(resources[i].GetRanges())
	}
	return x
}

func SumSets(rf resourcefilters.Filter, resources ...mesos.Resource) *mesos.Value_Set {
	predicate := resourcefilters.Filters{rf, resourcefilters.Set}
	var x *mesos.Value_Set
	for i := range resources {
		if !predicate.Accepts(&resources[i]) {
			continue
		}
		x = x.Add(resources[i].GetSet())
	}
	return x
}

func CPUs(resources ...mesos.Resource) (float64, bool) {
	v := SumScalars(resourcefilters.Named(ResourceNameCPUs), resources...)
	if v != nil {
		return v.Value, true
	}
	return 0, false
}

func GPUs(resources ...mesos.Resource) (float64, bool) {
	v := SumScalars(resourcefilters.Named(ResourceNameGPUs), resources...)
	if v != nil {
		return v.Value, true
	}
	return 0, false
}

func Memory(resources ...mesos.Resource) (uint64, bool) {
	v := SumScalars(resourcefilters.Named(ResourceNameMem), resources...)
	if v != nil {
		return uint64(v.Value), true
	}
	return 0, false
}

func Disk(resources ...mesos.Resource) (uint64, bool) {
	v := SumScalars(resourcefilters.Named(ResourceNameDisk), resources...)
	if v != nil {
		return uint64(v.Value), true
	}
	return 0, false
}

func Ports(resources ...mesos.Resource) (mesos.Ranges, bool) {
	v := SumRanges(resourcefilters.Named(ResourceNamePorts), resources...)
	if v != nil {
		return mesos.Ranges(v.Range), true
	}
	return nil, false
}

func Types(resources ...mesos.Resource) map[string]mesos.Value_Type {
	m := map[string]mesos.Value_Type{}
	for i := range resources {
		m[resources[i].GetName()] = resources[i].GetType()
	}
	return m
}

func Names(resources ...mesos.Resource) (names []string) {
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

func SumAndCompare(expected mesos.Resources, resources ...mesos.Resource) bool {
	// from: https://github.com/apache/mesos/blob/master/src/common/resources.cpp
	// This is a sanity check to ensure the amount of each type of
	// resource does not change.
	// TODO(jieyu): Currently, we only check known resource types like
	// cpus, mem, disk, ports, etc. We should generalize this.
	var (
		c1, c2 = CPUs(expected...)
		m1, m2 = Memory(expected...)
		d1, d2 = Disk(expected...)
		p1, p2 = Ports(expected...)
		g1, g2 = GPUs(expected...)

		c3, c4 = CPUs(resources...)
		m3, m4 = Memory(resources...)
		d3, d4 = Disk(resources...)
		p3, p4 = Ports(resources...)
		g3, g4 = GPUs(resources...)
	)
	return c1 == c3 && c2 == c4 &&
		m1 == m3 && m2 == m4 &&
		d1 == d3 && d2 == d4 &&
		g1 == g3 && g2 == g4 &&
		p1.Equivalent(p3) && p2 == p4
}

type (
	// functional option for resource flattening, via Flatten. Implementations are expected to type-narrow
	// the given interface, matching against WithRole, WithReservation or both methods (see Role.Assign).
	FlattenOpt func(interface{})

	flattenConfig struct {
		role        string
		reservation *mesos.Resource_ReservationInfo
	}
)

func (fc *flattenConfig) WithRole(role string)                              { fc.role = role }
func (fc *flattenConfig) WithReservation(r *mesos.Resource_ReservationInfo) { fc.reservation = r }

func Flatten(resources []mesos.Resource, opts ...FlattenOpt) (flattened mesos.Resources) {
	fc := &flattenConfig{}
	for _, f := range opts {
		f(fc)
	}
	if fc.role == "" {
		fc.role = string(RoleDefault)
	}
	// we intentionally manipulate a copy 'r' of the item in resources
	for _, r := range resources {
		r.Role = &fc.role
		r.Reservation = fc.reservation
		flattened.Add1(r)
	}
	return
}
