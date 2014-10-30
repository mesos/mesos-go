package main

import (
	"code.google.com/p/gogoprotobuf/proto"
	"flag"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/util"
	"strconv"
)

var ip = flag.String("ip", "127.0.0.1", "Master's ip address")
var port = flag.String("port", "5050", "Master's port")

type ExampleFramework struct {
	taskLimit int
	taskId    int
	exit      chan struct{}
	Scheduler *sched.Scheduler
}

var fw = &ExampleFramework{Scheduler: &sched.Scheduler{}}

func init() {
	log.Infoln("Initializing the Scheduler...")
	fw.Scheduler.Registered = func(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
		log.Infoln("Framework Registered with Master ", masterInfo)
	}

	fw.Scheduler.ResourceOffers = func(driver sched.SchedulerDriver, offers []*mesos.Offer) {
		log.Infoln("Got ", len(offers), " offers from master.")
		for _, offer := range offers {
			fw.taskId++
			log.Infoln("Launching task: %d\n", fw.taskId)

			tasks := []*mesos.TaskInfo{
				&mesos.TaskInfo{
					Name: proto.String("go-task"),
					TaskId: &mesos.TaskID{
						Value: proto.String("go-task-" + strconv.Itoa(fw.taskId)),
					},
					SlaveId:  offer.SlaveId,
					Executor: nil,
					Resources: []*mesos.Resource{
						util.NewScalarResource("cpus", 1),
						util.NewScalarResource("mem", 512),
					},
				},
			}

			driver.LaunchTasks(offer.Id, tasks, &mesos.Filters{})
		}
	}

	fw.Scheduler.Error = func(driver sched.SchedulerDriver, err string) {
		log.Infoln("Scheduler received error:", err)
	}
}

func main() {
	flag.Parse()
	log.Infoln("Starting scheduler test.")
	log.Infoln("Assuming master 127.0.0.1:5050...")

	master := *ip + ":" + *port

	fwinfo := &mesos.FrameworkInfo{
		User: proto.String("testuser"),
		Name: proto.String("sched-test"),
		Id:   &mesos.FrameworkID{Value: proto.String("mesos-framework-1")},
	}

	driver, err := sched.NewMesosSchedulerDriver(fw.Scheduler, fwinfo, master, nil)
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
