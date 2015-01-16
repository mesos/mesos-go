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

package scheduler

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"github.com/stretchr/testify/assert"
	"os"
	"os/user"
	"testing"
	"time"
)

var (
	master      = "127.0.0.1:8080"
	masterUpid  = "master(2)@" + master
	masterId    = "some-master-id-uuid"
	frameworkID = "some-framework-id-uuid"
	framework   = util.NewFrameworkInfo(
		"test-user",
		"test-name",
		util.NewFrameworkID(frameworkID),
	)
)

func TestSchedulerDriverNew(t *testing.T) {
	masterAddr := "localhost:5050"
	mUpid, err := upid.Parse("master@" + masterAddr)
	assert.NoError(t, err)
	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), &mesos.FrameworkInfo{}, masterAddr, nil)
	assert.NotNil(t, driver)
	assert.NoError(t, err)
	assert.True(t, driver.MasterPid.Equal(mUpid))
	user, _ := user.Current()
	assert.Equal(t, user.Username, driver.FrameworkInfo.GetUser())
	host, _ := os.Hostname()
	assert.Equal(t, host, driver.FrameworkInfo.GetHostname())
}

func TestSchedulerDriverNew_WithFrameworkInfo_Override(t *testing.T) {
	framework.Hostname = proto.String("local-host")
	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, "localhost:5050", nil)
	assert.NoError(t, err)
	assert.Equal(t, driver.FrameworkInfo.GetUser(), "test-user")
	assert.Equal(t, "local-host", driver.FrameworkInfo.GetHostname())
}

func TestSchedulerDriverStartOK(t *testing.T) {
	sched := NewMockScheduler()

	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(sched, framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	assert.False(t, driver.Stopped())
}

func TestSchedulerDriverStartWithMessengerFailure(t *testing.T) {
	sched := NewMockScheduler()
	sched.On("Error").Return()

	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(fmt.Errorf("Failed to start messenger"))
	messenger.On("Stop").Return()

	driver, err := NewMesosSchedulerDriver(sched, framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	stat, err := driver.Start()
	assert.Error(t, err)
	assert.True(t, driver.Stopped())
	assert.True(t, !driver.Connected())
	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, driver.Status())
	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, stat)

}

func TestSchedulerDriverStartWithRegistrationFailure(t *testing.T) {
	sched := NewMockScheduler()
	sched.On("Error").Return()

	// Set expections and return values.
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(fmt.Errorf("messenger failed to send"))
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(sched, framework, master, nil)

	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	stat, err := driver.Start()
	assert.Error(t, err)
	assert.True(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, driver.Status())
	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, stat)

}

func TestSchedulerDriverJoinUnstarted(t *testing.T) {
	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	stat, err := driver.Join()
	assert.Error(t, err)
	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, stat)
}

func TestSchedulerDriverJoinOK(t *testing.T) {
	// Set expections and return values.
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	assert.False(t, driver.Stopped())

	testCh := make(chan mesos.Status)
	go func() {
		stat, _ := driver.Join()
		testCh <- stat
	}()

	close(driver.stopCh) // manually stopping
	stat = <-testCh      // when Stop() is called, stat will be DRIVER_STOPPED.
}

func TestSchedulerDriverRun(t *testing.T) {
	// Set expections and return values.
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	go func() {
		stat, err := driver.Run()
		assert.NoError(t, err)
		assert.Equal(t, mesos.Status_DRIVER_STOPPED, stat)
	}()
	time.Sleep(time.Millisecond * 1)

	assert.False(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	// close it all.
	driver.setStatus(mesos.Status_DRIVER_STOPPED)
	close(driver.stopCh)
	time.Sleep(time.Millisecond * 1)
}

func TestSchedulerDriverStopUnstarted(t *testing.T) {
	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	stat, err := driver.Stop(true)
	assert.NotNil(t, err)
	assert.True(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, stat)
}

func TestSchdulerDriverStopOK(t *testing.T) {
	// Set expections and return values.
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	go func() {
		stat, err := driver.Run()
		assert.NoError(t, err)
		assert.Equal(t, mesos.Status_DRIVER_STOPPED, stat)
	}()
	time.Sleep(time.Millisecond * 1)

	assert.False(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	driver.Stop(false)
	time.Sleep(time.Millisecond * 1)

	assert.True(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_STOPPED, driver.Status())
}

func TestSchdulerDriverAbort(t *testing.T) {
	// Set expections and return values.
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	go func() {
		stat, err := driver.Run()
		assert.NoError(t, err)
		assert.Equal(t, mesos.Status_DRIVER_ABORTED, stat)
	}()
	time.Sleep(time.Millisecond * 1)
	driver.setConnected(true) // simulated

	assert.False(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	stat, err := driver.Abort()
	time.Sleep(time.Millisecond * 1)
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_ABORTED, stat)
	assert.Equal(t, mesos.Status_DRIVER_ABORTED, driver.Status())
}

func TestSchdulerDriverLunchTasksUnstarted(t *testing.T) {
	sched := NewMockScheduler()
	sched.On("Error").Return()

	// Set expections and return values.
	messenger := messenger.NewMockedMessenger()

	driver, err := NewMesosSchedulerDriver(sched, framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	stat, err := driver.LaunchTasks(
		[]*mesos.OfferID{&mesos.OfferID{}},
		[]*mesos.TaskInfo{},
		&mesos.Filters{},
	)
	assert.Error(t, err)
	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, stat)
}

func TestSchdulerDriverLaunchTasksWithError(t *testing.T) {
	sched := NewMockScheduler()
	sched.On("StatusUpdate").Return(nil)
	sched.On("Error").Return()

	msgr := messenger.NewMockedMessenger()
	msgr.On("Start").Return(nil)
	msgr.On("Send").Return(nil)
	msgr.On("UPID").Return(&upid.UPID{})
	msgr.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(sched, framework, master, nil)
	driver.messenger = msgr
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	go func() {
		driver.Run()
	}()
	time.Sleep(time.Millisecond * 1)
	driver.setConnected(true) // simulated
	assert.False(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	// to trigger error
	msgr2 := messenger.NewMockedMessenger()
	msgr2.On("Start").Return(nil)
	msgr2.On("UPID").Return(&upid.UPID{})
	msgr2.On("Send").Return(fmt.Errorf("Unable to send message"))
	msgr2.On("Stop").Return(nil)
	driver.messenger = msgr2

	// setup an offer
	offer := util.NewOffer(
		util.NewOfferID("test-offer-001"),
		framework.Id,
		util.NewSlaveID("test-slave-001"),
		"test-slave(1)@localhost:5050",
	)

	pid, err := upid.Parse("test-slave(1)@localhost:5050")
	assert.NoError(t, err)
	driver.cache.putOffer(offer, pid)

	// launch task
	task := util.NewTaskInfo(
		"simple-task",
		util.NewTaskID("simpe-task-1"),
		util.NewSlaveID("test-slave-001"),
		[]*mesos.Resource{util.NewScalarResource("mem", 400)},
	)
	task.Command = util.NewCommandInfo("pwd")
	task.Executor = util.NewExecutorInfo(util.NewExecutorID("test-exec"), task.Command)
	tasks := []*mesos.TaskInfo{task}

	stat, err := driver.LaunchTasks(
		[]*mesos.OfferID{offer.Id},
		tasks,
		&mesos.Filters{},
	)
	assert.Error(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)

}

func TestSchdulerDriverLaunchTasks(t *testing.T) {
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	go func() {
		driver.Run()
	}()
	time.Sleep(time.Millisecond * 1)
	driver.setConnected(true) // simulated
	assert.False(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	task := util.NewTaskInfo(
		"simple-task",
		util.NewTaskID("simpe-task-1"),
		util.NewSlaveID("slave-1"),
		[]*mesos.Resource{util.NewScalarResource("mem", 400)},
	)
	task.Command = util.NewCommandInfo("pwd")
	tasks := []*mesos.TaskInfo{task}

	stat, err := driver.LaunchTasks(
		[]*mesos.OfferID{&mesos.OfferID{}},
		tasks,
		&mesos.Filters{},
	)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
}

func TestSchdulerDriverKillTask(t *testing.T) {
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	go func() {
		driver.Run()
	}()
	time.Sleep(time.Millisecond * 1)
	driver.setConnected(true) // simulated
	assert.False(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	stat, err := driver.KillTask(util.NewTaskID("test-task-1"))
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
}

func TestSchdulerDriverRequestResources(t *testing.T) {
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	driver.Start()
	driver.setConnected(true) // simulated
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	stat, err := driver.RequestResources(
		[]*mesos.Request{
			&mesos.Request{
				SlaveId: util.NewSlaveID("test-slave-001"),
				Resources: []*mesos.Resource{
					util.NewScalarResource("test-res-001", 33.00),
				},
			},
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
}

func TestSchdulerDriverDeclineOffers(t *testing.T) {
	// see LaunchTasks test
}

func TestSchdulerDriverReviveOffers(t *testing.T) {
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	driver.Start()
	driver.setConnected(true) // simulated
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	stat, err := driver.ReviveOffers()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
}

func TestSchdulerDriverSendFrameworkMessage(t *testing.T) {
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	driver.Start()
	driver.setConnected(true) // simulated
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	stat, err := driver.SendFrameworkMessage(
		util.NewExecutorID("test-exec-001"),
		util.NewSlaveID("test-slave-001"),
		"Hello!",
	)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
}

func TestSchdulerDriverReconcileTasks(t *testing.T) {
	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	driver.Start()
	driver.setConnected(true) // simulated
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Status())

	stat, err := driver.ReconcileTasks(
		[]*mesos.TaskStatus{
			util.NewTaskStatus(util.NewTaskID("test-task-001"), mesos.TaskState_TASK_FINISHED),
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
}
