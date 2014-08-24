package scheduler

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/upid"
	"github.com/stretchr/testify/assert"
	"os"
	"os/user"
	"testing"
)

func TestSchedulerNew(t *testing.T) {
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

type noOpSched string
