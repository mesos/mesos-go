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

func (s *ExampleScheduler) Registered(driver SchedulerDriver, frameworkId FrameworkID, masterInfo MasterInfo) {
	return
}

func (s *ExampleScheduler) Reregistered(driver SchedulerDriver, masterInfo MasterInfo) {
	return
}

func (s *ExampleScheduler) Disconnected(driver SchedulerDriver) {
	return
}

func (s *ExampleScheduler) ResourceOffers(driver SchedulerDriver) {
	for _, offer := range offers {
		taskId++
		fmt.Printf("Launching task: %d\n", taskId)

		tasks := []mesos.TaskInfo{
			mesos.TaskInfo{
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

		driver.LaunchTasks(offer.Id, tasks)
	}
	return
}

func (s *ExampleScheduler) OfferRescinded(driver SchedulerDriver, offerId OfferID) {
	return
}

func (s *ExampleScheduler) StatusUpdate(driver SchedulerDriver, status TaskStatus) {
	fmt.Println("Received task status: " + *status.Message)

	if *status.State == mesos.TaskState_TASK_FINISHED {
		taskLimit--
		if taskLimit <= 0 {
			exit <- true
		}
	}
	return
}

func (s *ExampleScheduler) FrameworkMessage(driver SchedulerDriver, executorId ExecutorID, slaveId SlaveID, data string) {
	return
}

func (s *ExampleScheduler) SlaveLost(driver SchedulerDriver, slaveId SlaveID) {
	return
}

func (s *ExampleScheduler) ExecutorLost(driver SchedulerDriver, executorId ExecutorID, slaveId SlaveID, status int) {
	return
}

func (s *ExampleScheduler) Error(driver SchedulerDriver, message string) {
	return
}

func main() {
	taskLimit := 5
	taskId := 0
	exit := make(chan bool)
	localExecutor, _ := executorPath()

	master := flag.String("master", "localhost:5050", "Location of leading Mesos master")
	executorUri := flag.String("executor-uri", localExecutor, "URI of executor executable")
	flag.Parse()

	executor := &mesos.ExecutorInfo{
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

	driver := mesos.SchedulerDriver{
		Master: *master,
		Framework: mesos.FrameworkInfo{
			Name: proto.String("GoFramework"),
			User: proto.String(""),
		},

		Scheduler: ExampleScheduler,
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
