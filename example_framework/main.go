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

		Scheduler: &mesos.Scheduler{
			ResourceOffers: func(driver *mesos.SchedulerDriver, offers []mesos.Offer) {
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
			},

			StatusUpdate: func(driver *mesos.SchedulerDriver, status mesos.TaskStatus) {
				fmt.Println("Received task status: " + *status.Message)

				if *status.State == mesos.TaskState_TASK_FINISHED {
					taskLimit--
					if taskLimit <= 0 {
						exit <- true
					}
				}
			},
		},
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
