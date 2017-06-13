package resourcefilters

import (
	"github.com/mesos/mesos-go/api/v1/lib"
)

type (
	Interface interface {
		Accepts(*mesos.Resource) bool
	}
	Filter  func(*mesos.Resource) bool
	Filters []Interface
)

var _ = Interface(Filter(nil))

func (f Filter) Accepts(r *mesos.Resource) bool {
	if f != nil {
		return f(r)
	}
	return true
}

type any int

const Any = any(0)

func (_ any) Accepts(r *mesos.Resource) bool {
	return r != nil && !r.IsEmpty()
}

var _ = Interface(Any)

type unreserved int

const Unreserved = unreserved(0)

func (_ unreserved) Accepts(r *mesos.Resource) bool {
	return r.IsUnreserved()
}

var _ = Interface(Unreserved)

type persistentVolumes int

func (_ persistentVolumes) Accepts(r *mesos.Resource) bool {
	return r.IsPersistentVolume()
}

const PersistentVolumes = persistentVolumes(0)

var _ = Interface(PersistentVolumes)

type revocable int

func (_ revocable) Accepts(r *mesos.Resource) bool {
	return r.IsRevocable()
}

const Revocable = revocable(0)

var _ = Interface(Revocable)

type scalar int

func (_ scalar) Accepts(r *mesos.Resource) bool {
	return r.GetType() == mesos.SCALAR
}

const Scalar = scalar(0)

var _ = Interface(Scalar)

type rrange int

func (_ rrange) Accepts(r *mesos.Resource) bool {
	return r.GetType() == mesos.RANGES
}

const Range = rrange(0)

var _ = Interface(Range)

type set int

func (_ set) Accepts(r *mesos.Resource) bool {
	return r.GetType() == mesos.SET
}

const Set = set(0)

var _ = Interface(Set)

func (rf Filter) Or(f Filter) Filter {
	return Filter(func(r *mesos.Resource) bool {
		return rf(r) || f(r)
	})
}

func Select(rf Interface, resources ...mesos.Resource) (result mesos.Resources) {
	for i := range resources {
		if rf.Accepts(&resources[i]) {
			result.Add1(resources[i])
		}
	}
	return
}

func (rf Filters) Accepts(r *mesos.Resource) bool {
	for _, f := range rf {
		if !f.Accepts(r) {
			return false
		}
	}
	return true
}

var _ = Interface(Filters(nil))

func ReservedByRole(role string) Filter {
	return Filter(func(r *mesos.Resource) bool {
		return r.IsReserved(role)
	})
}

func Named(name string) Filter {
	return Filter(func(r *mesos.Resource) bool {
		return r.GetName() == name
	})
}
