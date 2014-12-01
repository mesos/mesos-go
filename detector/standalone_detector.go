package detector

import (
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type StandaloneMasterDetector struct {
	ch chan *mesos.MasterInfo
}

// Create a new stand alone master detector.
func NewStandaloneMasterDetector() *StandaloneMasterDetector {
	return &StandaloneMasterDetector{make(chan *mesos.MasterInfo)}
}

// Trigger a master detected event.
func (s *StandaloneMasterDetector) Appoint(m *mesos.MasterInfo) {
	log.V(2).Infoln("Appoint")
	s.ch <- m
}

// Detecting the new master.
func (s *StandaloneMasterDetector) Detect(receiver chan *mesos.MasterInfo) error {
	// go func() {
	// 	for {
	// 		receiver <- s.ch
	// 		log.V(2).Infoln("Master detected")
	// 	}
	// }()
	return nil
}

// Stop the detection.
func (s *StandaloneMasterDetector) Stop() {
	close(s.ch)
}
