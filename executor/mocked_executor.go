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
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesosphere/testify/mock"
)

// MockedExecutor is used for testing the executor driver.
type MockedExecutor struct {
	mock.Mock
}

// NewMockedExecutor returns a mocked executor.
func NewMockedExecutor() *MockedExecutor {
	return &MockedExecutor{}
}

// Registered implements the Registered handler.
func (e *MockedExecutor) Registered(ExecutorDriver, *mesosproto.ExecutorInfo, *mesosproto.FrameworkInfo, *mesosproto.SlaveInfo) {
	e.Mock.Called()
}

// Reregistered implements the Reregistered handler.
func (e *MockedExecutor) Reregistered(ExecutorDriver, *mesosproto.SlaveInfo) {
	e.Mock.Called()
}

// Disconnected implements the Disconnected handler.
func (e *MockedExecutor) Disconnected(ExecutorDriver) {
	e.Mock.Called()
}

// LaunchTask implements the LaunchTask handler.
func (e *MockedExecutor) LaunchTask(ExecutorDriver, *mesosproto.TaskInfo) {
	e.Mock.Called()
}

// KillTask implements the KillTask handler.
func (e *MockedExecutor) KillTask(ExecutorDriver, *mesosproto.TaskID) {
	e.Mock.Called()
}

// FrameworkMessage implements the FrameworkMessage handler.
func (e *MockedExecutor) FrameworkMessage(ExecutorDriver, string) {
	e.Mock.Called()
}

// Shutdown implements the Shutdown handler.
func (e *MockedExecutor) Shutdown(ExecutorDriver) {
	e.Mock.Called()
}

// Error implements the Error handler.
func (e *MockedExecutor) Error(ExecutorDriver, string) {
	e.Mock.Called()
}
