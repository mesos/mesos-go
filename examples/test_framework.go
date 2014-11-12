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

const (
	CPUS_PER_TASK = 1
	MEM_PER_TASK  = 128
)

var master = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
var execUri = flag.String("executor", "./test_executor", "Path to test executor")

type ExampleScheduler struct {
	executor      *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks    int
}

func newExampleScheduler(exec *mesos.ExecutorInfo) *ExampleScheduler {
	return &ExampleScheduler{
		executor:      exec,
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    10,
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
	log.Infoln("Received [", len(offers), "] offers total.")
	for _, offer := range offers {
		cpuResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "cpus"
		})
		cpus := 0.0
		for _, res := range cpuResources {
			cpus += res.GetScalar().GetValue()
		}

		memResources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
			return res.GetName() == "mem"
		})
		mems := 0.0
		for _, res := range memResources {
			mems += res.GetScalar().GetValue()
		}

		remainingCpus := cpus
		remainingMems := mems

		log.Infoln("Received Offer <", offer.Id.GetValue(), "> with cpus=", cpus, " mem=", mems)

		var tasks []*mesos.TaskInfo
		for sched.tasksLaunched < sched.totalTasks &&
			remainingCpus >= CPUS_PER_TASK &&
			remainingMems >= MEM_PER_TASK {
			sched.tasksLaunched++
			taskId := &mesos.TaskID{
				Value: proto.String("go-task-" + strconv.Itoa(sched.tasksLaunched)),
			}
			log.Infof("Launching task: %s with offer %s\n", taskId.GetValue(), offer.Id.GetValue())

			task := &mesos.TaskInfo{
				Name:     proto.String("go-task-" + taskId.GetValue()),
				TaskId:   taskId,
				SlaveId:  offer.SlaveId,
				Executor: sched.executor,
				Resources: []*mesos.Resource{
					util.NewScalarResource("cpus", CPUS_PER_TASK),
					util.NewScalarResource("mem", MEM_PER_TASK),
				},
			}

			tasks = append(tasks, task)
			remainingMems -= MEM_PER_TASK
			remainingCpus -= CPUS_PER_TASK
		}
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (sched *ExampleScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}
	if status.GetState() == mesos.TaskState_TASK_LOST ||
		status.GetState() == mesos.TaskState_TASK_KILLED ||
		status.GetState() == mesos.TaskState_TASK_FAILED {
		log.Infoln(
			"Aborting because task", status.TaskId.GetValue(),
			"is in unexpected state", status.State.String(),
			"with message", status.GetMessage(),
		)

		driver.Abort()
	}

	if sched.tasksFinished == sched.totalTasks {
		driver.Stop(false)
	}
}

func (sched *ExampleScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}

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

// ----------------------- func main() ------------------------- //
func main() {

	// build command executor
	exec := &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("default"),
		Name:       proto.String("Test Executor (Go)"),
		Source:     proto.String("go_test"),
		Command:    util.NewCommandInfo(*execUri),
	}

	// the framework
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(""), // Mesos-go will fill in user.
		Name: proto.String("Test Framework (Go)"),
	}

	driver, err := sched.NewMesosSchedulerDriver(
		newExampleScheduler(exec),
		fwinfo,
		*master,
		nil,
	)

	if err != nil {
		log.Errorln("Unable to create a SchedulerDriver ", err.Error())
	}

	if stat := driver.Run(); stat != mesos.Status_DRIVER_STOPPED {
		log.Infoln("A problem occured, framework reported status " + stat.String())
	}

}
