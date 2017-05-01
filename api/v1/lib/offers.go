package mesos

// Offers is a convenience type wrapper for a collection of mesos Offer messages
type Offers []Offer

// IDs extracts the ID field from the given list of offers
func (offers Offers) IDs() []OfferID {
	ids := make([]OfferID, len(offers))
	for i := range offers {
		ids[i] = offers[i].ID
	}
	return ids
}
