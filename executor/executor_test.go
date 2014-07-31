package executor

import (
	"os"
	"testing"
	"time"

	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/mockedSlave"
	"github.com/mesos/mesos-go/upid"
	"github.com/mesosphere/testify/assert"
)

var (
	slaveStarted = false
	slave        *mockedSlave.MockedSlave
	slavePID     = "slave(1)@127.0.0.1:8080"
	slaveID      = "some-slave-id-uuid"
	frameworkID  = "some-framework-id-uuid"
	executorID   = "some-executor-id-uuid"
)

func startMockedSlave(t *testing.T) *mockedSlave.MockedSlave {
	if !slaveStarted {
		upid, err := upid.Parse(slavePID)
		assert.NoError(t, err)
		slave = mockedSlave.NewMockedSlave(t, upid)
	}
	slave.Refresh()
	return slave
}

func setEnvironments(t *testing.T, workDir string, checkpoint bool) {
	assert.NoError(t, os.Setenv("MESOS_SLAVE_PID", slavePID))
	assert.NoError(t, os.Setenv("MESOS_SLAVE_ID", slaveID))
	assert.NoError(t, os.Setenv("MESOS_FRAMEWORK_ID", frameworkID))
	assert.NoError(t, os.Setenv("MESOS_EXECUTOR_ID", executorID))
	if len(workDir) > 0 {
		assert.NoError(t, os.Setenv("MESOS_DIRECTORY", workDir))
	}
	if checkpoint {
		assert.NoError(t, os.Setenv("MESOS_CHECKPOINT", "1"))
	}
}

func TestDriverFailToStart(t *testing.T) {
	driver := NewMesosExecutorDriver()
	driver.Executor = NewMockedExecutor()
	status, err := driver.Start()
	assert.Error(t, err)
	assert.Equal(t, mesosproto.Status_DRIVER_NOT_STARTED, status)
}

func TestDriverSucceedToStart(t *testing.T) {
	slave = startMockedSlave(t)
	slave.Mock.On("RegisterExecutor").Return()

	driver := NewMesosExecutorDriver()
	driver.Executor = NewMockedExecutor()
	setEnvironments(t, "", false)

	status, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesosproto.Status_DRIVER_RUNNING, status)

	time.Sleep(time.Second) // Sleep 1 seconde to wait for the slave to receive the message.
	slave.Mock.AssertNumberOfCalls(t, "RegisterExecutor", 1)
}
