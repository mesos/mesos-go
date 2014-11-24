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

package executor

import (
	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

// testScuduler is used for testing Schduler callbacks.
type testExecutor struct {
	ch chan bool
	wg *sync.WaitGroup
	t  *testing.T
}

func newTestExecutor(t *testing.T) *testExecutor {
	return &testExecutor{ch: make(chan bool), t: t}
}

func (exec *testExecutor) Registered(driver ExecutorDriver, execinfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveinfo *mesos.SlaveInfo) {
	log.Infoln("Sched.Registered() called.")
	exec.ch <- true
}

func (e *testExecutor) Reregistered(ExecutorDriver, *mesos.SlaveInfo) {}

func (e *testExecutor) Disconnected(ExecutorDriver) {}

func (e *testExecutor) LaunchTask(ExecutorDriver, *mesos.TaskInfo) {}

func (e *testExecutor) KillTask(ExecutorDriver, *mesos.TaskID) {}

func (e *testExecutor) FrameworkMessage(ExecutorDriver, string) {}

func (e *testExecutor) Shutdown(ExecutorDriver) {}

func (e *testExecutor) Error(ExecutorDriver, string) {}

// ------------------------ Test Functions -------------------- //

func setTestEnv(t *testing.T) {
	assert.NoError(t, os.Setenv("MESOS_FRAMEWORK_ID", frameworkID))
	assert.NoError(t, os.Setenv("MESOS_EXECUTOR_ID", executorID))
}

func TestExecutorDriverRegisterExecutorMessage(t *testing.T) {
	setTestEnv(t)
	ch := make(chan bool)
	server := util.NewMockSlaveHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		reqPath, err := url.QueryUnescape(req.URL.String())
		assert.NoError(t, err)
		log.Infoln("RCVD request", reqPath)

		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("Missing RegisteredExecutor data from scheduler.")
		}
		defer req.Body.Close()

		message := new(mesos.RegisterExecutorMessage)
		err = proto.Unmarshal(data, message)
		assert.NoError(t, err)
		assert.Equal(t, frameworkID, message.GetFrameworkId().GetValue())
		assert.Equal(t, executorID, message.GetExecutorId().GetValue())

		ch <- true

		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()

	exec := newTestExecutor(t)
	exec.ch = ch

	driver, err := NewMesosExecutorDriver(exec)
	assert.NoError(t, err)
	assert.True(t, driver.stopped)

	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.False(t, driver.stopped)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)

	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting...")
	}
}
