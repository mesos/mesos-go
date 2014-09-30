package main

import (
	"code.google.com/p/goprotobuf/proto"
	"flag"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
)

var Sched = &sched.Scheduler{}

func init() {
	log.Infoln("Initializing the Scheduler...")
	Sched.Registered = func(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
		log.Infoln("Framework Registered with Master ", masterInfo)
	}

	// Sched.Reregistered = func(driver *gomes.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	// 	log.Println("Framework Registered with Master ", masterInfo)
	// }

	// Sched.ResourceOffers = func(driver *gomes.SchedulerDriver, offers []*mesos.Offer) {
	// 	log.Println("Got ", len(offers), "offers from master.")
	// 	log.Println("Offer 1", offers[0])
	// }

	// Sched.Error = func(driver *gomes.SchedulerDriver, err gomes.MesosError) {
	// 	log.Println("Scheduler received error:", err.Error())
	// }
}

func main() {
	flag.Parse()
	log.Infoln("Starting scheduler test.")
	log.Infoln("Assuming master 127.0.0.1:5050...")

	master := "127.0.0.1:5050"

	framework := &mesos.FrameworkInfo{
		User: proto.String("testuser"),
		Name: proto.String("sched-test"),
		Id:   &mesos.FrameworkID{Value: proto.String("mesos-framework-1")},
	}

	driver, err := sched.NewMesosSchedulerDriver(Sched, framework, master, nil)
	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	ch := make(chan bool)

	go func() {
		stat := driver.Run()
		if stat != mesos.Status_DRIVER_STOPPED {
			log.Infoln("A problem occured, framework reported status " + stat.String())
		}
		ch <- true
	}()

	<-ch
}
