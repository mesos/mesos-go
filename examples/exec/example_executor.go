package main

import (
	"flag"
	"fmt"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/mesos/mesos-go/executor"
	"github.com/mesos/mesos-go/mesosproto"
)

type ExampleExecutor bool

func (exec *ExampleExecutor) Registered(executor.ExecutorDriver, *mesosproto.ExecutorInfo, *mesosproto.FrameworkInfo, *mesosproto.SlaveInfo) {
	fmt.Println("Executor Registered")
}

func (exec *ExampleExecutor) Reregistered(executor.ExecutorDriver, *mesosproto.SlaveInfo) {
	fmt.Println("Executor Reregistered on")
}

func (exec *ExampleExecutor) Disconnected(executor.ExecutorDriver) {
	fmt.Println("Executor disconnected")
}

func (exec *ExampleExecutor) LaunchTask(driver executor.ExecutorDriver, taskInfo *mesosproto.TaskInfo) {
	fmt.Println("Starting task", taskInfo.GetName())
	runningState := mesosproto.TaskState_TASK_RUNNING
	running := &mesosproto.TaskStatus{
		TaskId:     taskInfo.GetTaskId(),
		State:      &runningState,
		SlaveId:    taskInfo.GetSlaveId(),
		ExecutorId: taskInfo.GetExecutor().GetExecutorId(),
		Message:    proto.String("Native go task is running!"),
	}
	_, err := driver.SendStatusUpdate(running)
	if err != nil {
		fmt.Println("Got error", err)
	}

	fmt.Println("Finishing task", taskInfo.GetName())
	finishedState := mesosproto.TaskState_TASK_FINISHED
	finished := &mesosproto.TaskStatus{
		TaskId:     taskInfo.GetTaskId(),
		State:      &finishedState,
		SlaveId:    taskInfo.GetSlaveId(),
		ExecutorId: taskInfo.GetExecutor().GetExecutorId(),
		Message:    proto.String("Native go task is done!"),
	}
	_, err = driver.SendStatusUpdate(finished)
	if err != nil {
		fmt.Println("Got error", err)
	}
	fmt.Println("Task finished", taskInfo.GetName())
}

func (exec *ExampleExecutor) KillTask(executor.ExecutorDriver, *mesosproto.TaskID) {
	fmt.Println("Kill task")
}

func (exec *ExampleExecutor) FrameworkMessage(executor.ExecutorDriver, string) {
	fmt.Println("Got framework message")
}

func (exec *ExampleExecutor) Shutdown(executor.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (exec *ExampleExecutor) Error(executor.ExecutorDriver, string) {
	fmt.Println("Got error message")
}

func main() {
	flag.Parse()
	driver := executor.NewMesosExecutorDriver()
	if driver == nil {
		return
	}
	driver.Executor = new(ExampleExecutor)
	_, err := driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	driver.Join()
}
