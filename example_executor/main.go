package main

import (
	"fmt"

	"code.google.com/p/goprotobuf/proto"
	"github.com/mesosphere/mesos-go/mesos"
)

func main() {
	driver := mesos.ExecutorDriver{
		Executor: &mesos.Executor{
			Registered: func(
				driver *mesos.ExecutorDriver,
				executor mesos.ExecutorInfo,
				framework mesos.FrameworkInfo,
				slave mesos.SlaveInfo) {
				fmt.Println("Executor registered!")
			},

			LaunchTask: func(driver *mesos.ExecutorDriver, taskInfo mesos.TaskInfo) {
				fmt.Println("Launch task!")
				driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskInfo.TaskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_RUNNING),
					Message: proto.String("Go task is running!"),
				})

				driver.SendStatusUpdate(&mesos.TaskStatus{
					TaskId:  taskInfo.TaskId,
					State:   mesos.NewTaskState(mesos.TaskState_TASK_FINISHED),
					Message: proto.String("Go task is done!"),
				})
			},
		},
	}

	driver.Init()
	defer driver.Destroy()

	driver.Run()
}
