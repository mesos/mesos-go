package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"code.google.com/p/goprotobuf/proto"
	"github.com/mesosphere/mesos-go/mesos"
)

type ExampleScheduler bool

var taskLimit = 5
var taskId = 0
var exit = make(chan bool)
var executor *mesos.ExecutorInfo

func (s *ExampleScheduler) Registered(driver mesos.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	return
}

func (s *ExampleScheduler) Reregistered(driver mesos.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	return
}

func (s *ExampleScheduler) Disconnected(driver mesos.SchedulerDriver) {
	return
}

func (s *ExampleScheduler) ResourceOffers(driver mesos.SchedulerDriver, offers []*mesos.Offer) {
	for _, offer := range offers {
		taskId++
		fmt.Printf("Launching task: %d\n", taskId)

		tasks := []*mesos.TaskInfo{
			&mesos.TaskInfo{
				Name: proto.String("go-task"),
				TaskId: &mesos.TaskID{
					Value: proto.String("go-task-" + strconv.Itoa(taskId)),
				},
				SlaveId:  offer.SlaveId,
				Executor: executor,
				Resources: []*mesos.Resource{
					mesos.ScalarResource("cpus", 1),
					mesos.ScalarResource("mem", 512),
				},
			},
		}

		driver.LaunchTasks(offer.Id, tasks, nil)
	}
	return
}

func (s *ExampleScheduler) OfferRescinded(driver mesos.SchedulerDriver, offerId *mesos.OfferID) {
	return
}

func (s *ExampleScheduler) StatusUpdate(driver mesos.SchedulerDriver, status *mesos.TaskStatus) {
	fmt.Println("Received task status: " + *status.Message)

	if *status.State == mesos.TaskState_TASK_FINISHED {
		taskLimit--
		if taskLimit <= 0 {
			exit <- true
		}
	}
	return
}

func (s *ExampleScheduler) FrameworkMessage(driver mesos.SchedulerDriver, executorId *mesos.ExecutorID, slaveId *mesos.SlaveID, data string) {
	return
}

func (s *ExampleScheduler) SlaveLost(driver mesos.SchedulerDriver, slaveId *mesos.SlaveID) {
	return
}

func (s *ExampleScheduler) ExecutorLost(driver mesos.SchedulerDriver, executorId *mesos.ExecutorID, slaveId *mesos.SlaveID, status int) {
	return
}

func (s *ExampleScheduler) Error(driver mesos.SchedulerDriver, message string) {
	return
}

func main() {
	localExecutor, _ := executorPath()

	master := flag.String("master", "localhost:5050", "Location of leading Mesos master")
	executorUri := flag.String("executor-uri", localExecutor, "URI of executor executable")
	flag.Parse()

	executor = &mesos.ExecutorInfo{
		ExecutorId: &mesos.ExecutorID{Value: proto.String("default")},
		Command: &mesos.CommandInfo{
			Value: proto.String("./example_executor"),
			Uris: []*mesos.CommandInfo_URI{
				&mesos.CommandInfo_URI{Value: executorUri},
			},
		},
		Name:   proto.String("Test Executor (Go)"),
		Source: proto.String("go_test"),
	}

	exampleScheduler := new(ExampleScheduler)
	driver := &mesos.MesosSchedulerDriver{
		Master: *master,
		Framework: mesos.FrameworkInfo{
			Name: proto.String("GoFramework"),
			User: proto.String(""),
		},

		Scheduler: exampleScheduler,
	}

	driver.Init()
	defer driver.Destroy()

	driver.Start()
	<-exit
	driver.Stop(false)
}

func executorPath() (string, error) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return "", err
	}

	path := dir + "/example_executor"
	return path, nil
}
