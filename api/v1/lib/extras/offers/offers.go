package offers

import . "github.com/mesos/mesos-go/api/v1/lib"

type (
	// Slice is a convenience type wrapper for a slice of mesos Offer messages
	Slice []Offer

	// Index is a convenience type wrapper for a dictionary of Offer messages
	Index map[interface{}]*Offer

	// KeyFunc generates a key used for indexing offers
	KeyFunc func(*Offer) interface{}
)

// IDs extracts the ID field from a Slice of offers
func (offers Slice) IDs() []OfferID {
	ids := make([]OfferID, len(offers))
	for i := range offers {
		ids[i] = offers[i].ID
	}
	return ids
}

// IDs extracts the ID field from a Index of offers
func (offers Index) IDs() []OfferID {
	ids := make([]OfferID, 0, len(offers))
	for _, offer := range offers {
		ids = append(ids, offer.GetID())
	}
	return ids
}

// Find returns the first Offer that passes the given filter function.
func (offers Slice) Find(filter Filter) *Offer {
	for i := range offers {
		offer := &offers[i]
		if filter.Accept(offer) {
			return offer
		}
	}
	return nil
}

// Find returns the first Offer that passes the given filter function.
func (offers Index) Find(filter Filter) *Offer {
	for _, offer := range offers {
		if filter.Accept(offer) {
			return offer
		}
	}
	return nil
}

// Filter returns the subset of the Slice that matches the given filter.
func (offers Slice) Filter(filter Filter) (result Slice) {
	if sz := len(result); sz > 0 {
		result = make(Slice, 0, sz)
		for i := range offers {
			if filter.Accept(&offers[i]) {
				result = append(result, offers[i])
			}
		}
	}
	return
}

// Filter returns the subset of the Index that matches the given filter.
func (offers Index) Filter(filter Filter) (result Index) {
	if sz := len(result); sz > 0 {
		result = make(Index, sz)
		for id, offer := range offers {
			if filter.Accept(offer) {
				result[id] = offer
			}
		}
	}
	return
}

// FilterNot returns the subset of the Slice that does not match the given filter.
func (offers Slice) FilterNot(filter Filter) Slice { return offers.Filter(not(filter)) }

// FilterNot returns the subset of the Index that does not match the given filter.
func (offers Index) FilterNot(filter Filter) Index { return offers.Filter(not(filter)) }

// ContainsResources returns a filter function that returns true if the Resources of an Offer
// contain the wanted Resources.
func ContainsResources(wanted Resources) Filter {
	return FilterFunc(func(o *Offer) bool {
		return Resources(o.Resources).Flatten().ContainsAll(wanted)
	})
}

// DefaultKeyFunc indexes offers by their OfferID.
var DefaultKeyFunc = KeyFunc(func(o *Offer) interface{} { return o.GetID() })

// NewIndex returns a new Index constructed from the list of mesos offers.
// If the KeyFunc is nil then offers are indexed by DefaultKeyFunc.
// The values of the returned Index are pointers to (not copies of) the offers of the slice receiver.
func NewIndex(slice []Offer, kf KeyFunc) Index {
	if slice == nil {
		return nil
	}
	if kf == nil {
		kf = DefaultKeyFunc
	}
	index := make(Index, len(slice))
	for i := range slice {
		offer := &slice[i]
		index[kf(offer)] = offer
	}
	return index
}

// ToSlice returns a Slice from the offers in the Index.
// The returned slice will contain shallow copies of the offers from the Index.
func (s Index) ToSlice() (slice Slice) {
	if sz := len(s); sz > 0 {
		slice = make(Slice, 0, sz)
		for _, offer := range s {
			slice = append(slice, *offer)
		}
	}
	return
}

type (
	Reducer interface {
		Reduce(_, _ *Offer) *Offer
	}

	ReduceFunc func(_, _ *Offer) *Offer
)

func (f ReduceFunc) Reduce(a, b *Offer) *Offer { return f(a, b) }

var _ = Reducer(ReduceFunc(func(_, _ *Offer) *Offer { return nil })) // sanity check

func (slice Slice) Reduce(def Offer, r Reducer) (result Offer) {
	result = def
	if r != nil {
		acc := &result
		for i := range slice {
			acc = r.Reduce(&result, &slice[i])
		}
		if acc == nil {
			result = Offer{}
		} else {
			result = *acc
		}
	}
	return
}

func (index Index) Reduce(def *Offer, r Reducer) (result *Offer) {
	result = def
	if r != nil {
		for i := range index {
			result = r.Reduce(result, index[i])
		}
	}
	return
}

func (slice Slice) GroupBy(kf KeyFunc) map[interface{}]Slice {
	if kf == nil {
		panic("keyFunc must not be nil")
	}
	if len(slice) == 0 {
		return nil
	}
	result := make(map[interface{}]Slice)
	for i := range slice {
		groupKey := kf(&slice[i])
		result[groupKey] = append(result[groupKey], slice[i])
	}
	return result
}

func (index Index) GroupBy(kf KeyFunc) map[interface{}]Slice {
	if kf == nil {
		panic("keyFunc must not be nil")
	}
	if len(index) == 0 {
		return nil
	}
	result := make(map[interface{}]Slice)
	for _, offer := range index {
		groupKey := kf(offer)
		result[groupKey] = append(result[groupKey], *offer)
	}
	return result
}

func (index Index) Partition(f Filter) (accepted, rejected Index) {
	if f == nil {
		return index, nil
	}
	if len(index) > 0 {
		accepted, rejected = make(Index), make(Index)
		for id, offer := range index {
			if f.Accept(offer) {
				accepted[id] = offer
			} else {
				rejected[id] = offer
			}
		}
	}
	return
}

func (index Index) Reindex(kf KeyFunc) Index {
	sz := len(index)
	if kf == nil || sz == 0 {
		return index
	}
	result := make(Index, sz)
	for _, offer := range index {
		key := kf(offer)
		result[key] = offer
	}
	return result
}
