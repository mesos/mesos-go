package detector

import (
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type Standalone struct {
	current *mesos.MasterInfo
	ch      chan *mesos.MasterInfo
}

// Create a new stand alone master detector.
func NewStandalone(mi *mesos.MasterInfo) *Standalone {
	return &Standalone{current: mi, ch: make(chan *mesos.MasterInfo)}
}

// Trigger a master detected event.
func (s *Standalone) Appoint(m *mesos.MasterInfo) {
	log.V(2).Infoln("Appoint")
	s.ch <- m
	s.current = m
}

// Detecting the new master.
func (s *Standalone) Detect(o MasterChanged) error {
	// go func() {
	// 	for {
	// 		receiver <- s.ch
	// 		log.V(2).Infoln("Master detected")
	// 	}
	// }()
	return nil
}

// Stop the detection.
func (s *Standalone) Stop() {
	select {
	case <-s.ch:
		// already closed, don't close it again
	default:
		close(s.ch)
	}
}
