package main

import (
	"code.google.com/p/gogoprotobuf/proto"
	"flag"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"
	"strconv"
)

var ip = flag.String("ip", "127.0.0.1", "Master's ip address")
var port = flag.String("port", "5050", "Master's port")

type ExampleScheduler struct {
	taskLimit int
	taskId    int
	exit      chan struct{}
}

func newExampleScheduler() *ExampleScheduler {
	return &ExampleScheduler{
		taskLimit: 10,
		taskId:    0,
		exit:      make(chan struct{}),
	}
}

func (sched *ExampleScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
}

func (sched *ExampleScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}

func (sched *ExampleScheduler) Disconnected(sched.SchedulerDriver) {}

func (sched *ExampleScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	log.Infoln("Got ", len(offers), " offers from master.")
	for _, offer := range offers {
		sched.taskId++
		log.Infoln("Launching task: %d\n", sched.taskId)

		tasks := []*mesos.TaskInfo{
			&mesos.TaskInfo{
				Name: proto.String("go-task"),
				TaskId: &mesos.TaskID{
					Value: proto.String("go-task-" + strconv.Itoa(sched.taskId)),
				},
				SlaveId:  offer.SlaveId,
				Executor: nil,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", 1),
					util.NewScalarResource("mem", 512),
				},
			},
		}

		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{})
	}
}

func (sched *ExampleScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID)  {}
func (sched *ExampleScheduler) StatusUpdate(sched.SchedulerDriver, *mesos.TaskStatus) {}
func (sched *ExampleScheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, []byte) {
}
func (sched *ExampleScheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {}
func (sched *ExampleScheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

func (sched *ExampleScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}

func init() {
	flag.Parse()
	log.Infoln("Initializing the Example Scheduler...")
}

func main() {
	master := *ip + ":" + *port

	fwinfo := &mesos.FrameworkInfo{
		User: proto.String("testuser"),
		Name: proto.String("sched-test"),
		Id:   &mesos.FrameworkID{Value: proto.String("mesos-framework-1")},
	}

	driver, err := sched.NewMesosSchedulerDriver(newExampleScheduler(), fwinfo, master, nil)
	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	ch := make(chan bool)

	go func() {
		log.Infoln("Starting scheduler: with master ", master)
		stat := driver.Run()
		if stat != mesos.Status_DRIVER_STOPPED {
			log.Infoln("A problem occured, framework reported status " + stat.String())
		}
		ch <- true
	}()

	<-ch
}
