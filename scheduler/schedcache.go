package scheduler

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/upid"
	"sync"
)

type cachedOffer struct {
	offer    *mesos.Offer
	slavePid *upid.UPID
}

// schedCache a managed cache with backing maps to store offeres
// and tasked slaves.
type schedCache struct {
	sync.RWMutex
	savedOffers    map[string]cachedOffer // current offers key:OfferID
	savedSlavePids map[string]*upid.UPID  // Current saved slaves, key:slaveId
}

func newSchedCache() *schedCache {
	return &schedCache{
		savedOffers:    make(map[string]cachedOffer),
		savedSlavePids: make(map[string]*upid.UPID),
	}
}

// putOffer stores an offer and the slavePID associated with offer.
func (cache *schedCache) putOffer(offer *mesos.Offer, pid *upid.UPID) {
	cache.savedOffers[offer.Id.GetValue()] = cachedOffer{offer: offer, slavePid: pid}
}

// getOffer returns cached offer
func (cache *schedCache) getOffer(offerId *mesos.OfferID) cachedOffer {
	if offerId == nil {
		return cachedOffer{}
	}
	return cache.savedOffers[offerId.GetValue()]
}

// containsOff test cache for offer(offerId)
func (cache *schedCache) containsOffer(offerId *mesos.OfferID) bool {
	_, ok := cache.savedOffers[offerId.GetValue()]
	return ok
}

func (cache *schedCache) removeOffer(offerId *mesos.OfferID) {
	delete(cache.savedOffers, offerId.GetValue())
}

func (cache *schedCache) putSlavePid(slaveId *mesos.SlaveID, pid *upid.UPID) {
	cache.savedSlavePids[slaveId.GetValue()] = pid
}

func (cache *schedCache) getSlavePid(slaveId *mesos.SlaveID) *upid.UPID {
	if slaveId == nil {
		return &upid.UPID{}
	}
	return cache.savedSlavePids[slaveId.GetValue()]
}

func (cache *schedCache) containsSlavePid(slaveId *mesos.SlaveID) bool {
	_, ok := cache.savedSlavePids[slaveId.GetValue()]
	return ok
}

func (cache *schedCache) removeSlavePid(slaveId *mesos.SlaveID) {
	delete(cache.savedSlavePids, slaveId.GetValue())
}
