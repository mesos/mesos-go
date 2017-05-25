package offers

import . "github.com/mesos/mesos-go/api/v1/lib"

type (
	// Filter accepts or rejects a mesos Offer
	Filter interface {
		Accept(*Offer) bool
	}

	// FilterFunc returns true if the given Offer passes the filter
	FilterFunc func(*Offer) bool
)

// Accept implements Filter for FilterFunc
func (f FilterFunc) Accept(o *Offer) bool { return f(o) }

func nilFilter(_ *Offer) bool { return true }

func not(f Filter) Filter {
	return FilterFunc(func(offer *Offer) bool { return !f.Accept(offer) })
}

// ByHostname returns a Filter that accepts offers with a matching Hostname
func ByHostname(hostname string) Filter {
	if hostname == "" {
		return FilterFunc(nilFilter)
	}
	return FilterFunc(func(o *Offer) bool {
		return o.Hostname == hostname
	})
}

// ByAttributes returns a Filter that accepts offers with an attribute set accepted by
// the provided Attribute filter func.
func ByAttributes(f func(attr []Attribute) bool) Filter {
	if f == nil {
		return FilterFunc(nilFilter)
	}
	return FilterFunc(func(o *Offer) bool {
		return f(o.Attributes)
	})
}

func ByExecutors(f func(exec []ExecutorID) bool) Filter {
	if f == nil {
		return FilterFunc(nilFilter)
	}
	return FilterFunc(func(o *Offer) bool {
		return f(o.ExecutorIDs)
	})
}

func ByUnavailability(f func(u *Unavailability) bool) Filter {
	if f == nil {
		return FilterFunc(nilFilter)
	}
	return FilterFunc(func(o *Offer) bool {
		return f(o.Unavailability)
	})
}

// ContainsResources returns a filter function that returns true if the Resources of an Offer
// contain the wanted Resources.
func ContainsResources(wanted Resources) Filter {
	return FilterFunc(func(o *Offer) bool {
		return Resources(o.Resources).Flatten().ContainsAll(wanted)
	})
}
