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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mesos/mesos-go/healthchecker"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"github.com/mesosphere/testify/assert"
)

var (
	slavePID    = "slave(1)@127.0.0.1:8080"
	slaveID     = "some-slave-id-uuid"
	frameworkID = "some-framework-id-uuid"
	executorID  = "some-executor-id-uuid"
)

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

func createTestExecutorDriver(t *testing.T) (
	*MesosExecutorDriver,
	*messenger.MockedMessenger,
	*healthchecker.MockedHealthChecker) {

	setEnvironments(t, "", false)
	driver := NewMesosExecutorDriver()
	assert.NotNil(t, driver)

	messenger := messenger.NewMockedMessenger()
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)

	checker := healthchecker.NewMockedHealthChecker()
	checker.On("Start").Return()

	driver.messenger, driver.slaveHealthChecker = messenger, checker
	return driver, messenger, checker
}

func TestExecutorDriverStartFailedToParseEnvironment(t *testing.T) {
	driver := NewMesosExecutorDriver()
	assert.Nil(t, driver)
}

func TestExecutorDriverStartFailedToStartMessenger(t *testing.T) {
	setEnvironments(t, "", false)
	driver := NewMesosExecutorDriver()
	assert.NotNil(t, driver)
	messenger := messenger.NewMockedMessenger()
	driver.messenger = messenger
	driver.slaveHealthChecker = healthchecker.NewMockedHealthChecker()

	// Set expections and return values.
	messenger.On("Start").Return(fmt.Errorf("messenger failed to start"))

	status, err := driver.Start()
	assert.Error(t, err)
	assert.Equal(t, mesosproto.Status_DRIVER_NOT_STARTED, status)

	messenger.AssertNumberOfCalls(t, "Start", 1)
}

func TestExecutorDriverStartFailedToSendRegisterMessage(t *testing.T) {
	setEnvironments(t, "", false)
	driver := NewMesosExecutorDriver()
	assert.NotNil(t, driver)
	messenger := messenger.NewMockedMessenger()
	driver.messenger = messenger
	driver.slaveHealthChecker = healthchecker.NewMockedHealthChecker()

	// Set expections and return values.
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(fmt.Errorf("messenger failed to send"))
	messenger.On("Stop").Return(nil)

	status, err := driver.Start()
	assert.Error(t, err)
	assert.Equal(t, mesosproto.Status_DRIVER_NOT_STARTED, status)

	messenger.AssertNumberOfCalls(t, "Start", 1)
	messenger.AssertNumberOfCalls(t, "UPID", 1)
	messenger.AssertNumberOfCalls(t, "Send", 1)
	messenger.AssertNumberOfCalls(t, "Stop", 1)
}

func TestExecutorDriverStartSucceed(t *testing.T) {
	setEnvironments(t, "", false)
	driver := NewMesosExecutorDriver()
	assert.NotNil(t, driver)
	messenger := messenger.NewMockedMessenger()
	driver.messenger = messenger
	checker := healthchecker.NewMockedHealthChecker()
	driver.slaveHealthChecker = checker

	// Set expections and return values.
	messenger.On("Start").Return(nil)
	messenger.On("UPID").Return(&upid.UPID{})
	messenger.On("Send").Return(nil)
	messenger.On("Stop").Return(nil)
	checker.On("Start").Return(nil)

	status, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesosproto.Status_DRIVER_RUNNING, status)

	status, err = driver.Stop()
	assert.NoError(t, err)
	assert.Equal(t, mesosproto.Status_DRIVER_STOPPED, status)

	// Sleep 1 second to enable checker.Start() be called.
	time.Sleep(time.Second)

	messenger.AssertNumberOfCalls(t, "Start", 1)
	messenger.AssertNumberOfCalls(t, "UPID", 1)
	messenger.AssertNumberOfCalls(t, "Send", 1)
	messenger.AssertNumberOfCalls(t, "Stop", 1)
	checker.AssertNumberOfCalls(t, "Start", 1)
}

func TestExecutorDriverStop(t *testing.T) {
	statusChan := make(chan mesosproto.Status)
	driver, messenger, checker := createTestExecutorDriver(t)
	go func() {
		st, err := driver.Join()
		assert.NoError(t, err)
		statusChan <- st
	}()
	st, err := driver.Stop()
	assert.NoError(t, err)
	assert.Equal(t, mesosproto.Status_DRIVER_NOT_STARTED, st)
	assert.Equal(t, mesosproto.Status_DRIVER_NOT_STARTED, <-statusChan)

	st, err = driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesosproto.Status_DRIVER_RUNNING, st)
	go func() {
		st, err := driver.Join()
		assert.NoError(t, err)
		statusChan <- st
	}()
	st, err = driver.Stop()
	assert.Equal(t, mesosproto.Status_DRIVER_STOPPED, st)
	assert.Equal(t, mesosproto.Status_DRIVER_STOPPED, <-statusChan)

	messenger.AssertNumberOfCalls(t, "Start", 1)
	messenger.AssertNumberOfCalls(t, "UPID", 1)
	messenger.AssertNumberOfCalls(t, "Send", 1)
	messenger.AssertNumberOfCalls(t, "Stop", 1)
	checker.AssertNumberOfCalls(t, "Start", 1)
}
