package scheduler

import (
	"code.google.com/p/gogoprotobuf/proto"
	"fmt"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"github.com/mesos/mesos-go/util"
	"github.com/stretchr/testify/assert"
	"os"
	"os/user"
	"testing"
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
	mUpid, err := upid.Parse("master(1)@" + masterAddr)
	assert.NoError(t, err)
	driver, err := NewMesosSchedulerDriver(&Scheduler{}, &mesos.FrameworkInfo{}, masterAddr, nil)
	assert.NotNil(t, driver)
	assert.NoError(t, err)
	assert.True(t, driver.MasterUPID.Equal(mUpid))
	user, _ := user.Current()
	assert.Equal(t, user.Username, driver.FrameworkInfo.GetUser())
	host, _ := os.Hostname()
	assert.Equal(t, host, driver.FrameworkInfo.GetHostname())
}

func TestSchedulerDriverNew_WithFrameworkInfo_Override(t *testing.T) {
	framework.Hostname = proto.String("local-host")
	driver, err := NewMesosSchedulerDriver(&Scheduler{}, framework, "localhost:5050", nil)
	assert.NoError(t, err)
	assert.Equal(t, driver.FrameworkInfo.GetUser(), "test-user")
	assert.Equal(t, "local-host", driver.FrameworkInfo.GetHostname())
}

func TestSchedulerDriverStartOK(t *testing.T) {
	sched := &Scheduler{}
	sched.Registered = func(SchedulerDriver, *mesos.FrameworkID, *mesos.MasterInfo) {

	}

	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	driver, err := NewMesosSchedulerDriver(sched, framework, master, nil)
	driver.messenger = messenger
	assert.NoError(t, err)
	assert.True(t, driver.stopped)

	fmt.Println("Starting sched driver...")
	stat := driver.Start()
	assert.False(t, driver.stopped)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
}

// func TestSchedulerDriverStartWithMessengerFailure(t *testing.T) {
// 	sched := &Scheduler{}

// 	messenger := messenger.NewMockedMessenger()
// 	messenger.On("Start").Return(fmt.Errorf("Failed to start messenger"))
// 	messenger.On("Stop").Return()

// 	driver, err := NewMesosSchedulerDriver(sched, framework, master, nil)
// 	driver.messenger = messenger
// 	assert.NoError(t, err)
// 	assert.True(t, driver.stopped)

// 	stat := driver.Start()
// 	assert.True(t, driver.stopped)
// 	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, driver.status)
// 	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, stat)

// }

// func TestSchedulerDriverStartWithRegistrationFailure(t *testing.T) {
// 	sched := &Scheduler{}

// 	// Set expections and return values.
// 	messenger := messenger.NewMockedMessenger()
// 	messenger.On("Start").Return(nil)
// 	messenger.On("UPID").Return(&upid.UPID{})
// 	messenger.On("Send").Return(fmt.Errorf("messenger failed to send"))
// 	messenger.On("Stop").Return(nil)

// 	driver, err := NewMesosSchedulerDriver(sched, framework, master, nil)
// 	driver.messenger = messenger
// 	assert.NoError(t, err)
// 	assert.True(t, driver.stopped)

// 	stat := driver.Start()
// 	assert.True(t, driver.stopped)
// 	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, driver.status)
// 	assert.Equal(t, mesos.Status_DRIVER_NOT_STARTED, stat)

// }