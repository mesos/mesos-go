// +build test-sched

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
var taskCount = flag.String("task-count", "5", "Total task count to run.")

type ExampleScheduler struct {
	executor      *mesos.ExecutorInfo
	tasksLaunched int
	tasksFinished int
	totalTasks    int
}

func newExampleScheduler(exec *mesos.ExecutorInfo) *ExampleScheduler {
	total, err := strconv.Atoi(*taskCount)
	if err != nil {
		total = 5
	}
	return &ExampleScheduler{
		executor:      exec,
		tasksLaunched: 0,
		tasksFinished: 0,
		totalTasks:    total,
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

		log.Infoln("Received Offer <", offer.Id.GetValue(), "> with cpus=", cpus, " mem=", mems)

		remainingCpus := cpus
		remainingMems := mems

		var tasks []*mesos.TaskInfo
		for sched.tasksLaunched <= sched.totalTasks &&
			CPUS_PER_TASK <= remainingCpus &&
			MEM_PER_TASK <= remainingMems {

			sched.tasksLaunched++

			taskId := &mesos.TaskID{
				Value: proto.String(strconv.Itoa(sched.tasksLaunched)),
			}

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
			log.Infof("Prepared task: %s with offer %s for launch\n", task.GetName(), offer.Id.GetValue())

			tasks = append(tasks, task)
			remainingCpus -= CPUS_PER_TASK
			remainingMems -= MEM_PER_TASK
		}
		log.Infoln("Launching ", len(tasks), "tasks for offer", offer.Id.GetValue())
		driver.LaunchTasks([]*mesos.OfferID{offer.Id}, tasks, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
	}
}

func (sched *ExampleScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), " is in state ", status.State.Enum().String())
	if status.GetState() == mesos.TaskState_TASK_FINISHED {
		sched.tasksFinished++
	}

	if sched.tasksFinished >= sched.totalTasks {
		log.Infoln("Total tasks completed, stopping framework.")
		driver.Stop(false)
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
}

func (sched *ExampleScheduler) OfferRescinded(sched.SchedulerDriver, *mesos.OfferID) {}

func (sched *ExampleScheduler) FrameworkMessage(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
}
func (sched *ExampleScheduler) SlaveLost(sched.SchedulerDriver, *mesos.SlaveID) {}
func (sched *ExampleScheduler) ExecutorLost(sched.SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
}

func (sched *ExampleScheduler) Error(driver sched.SchedulerDriver, err string) {
	log.Infoln("Scheduler received error:", err)
}

// ----------------------- func init() ------------------------- //

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

	if stat, err := driver.Run(); err != nil {
		log.Infof("Framework stopped with status %s and error: %s\n", stat.String(), err.Error())
	}

}
