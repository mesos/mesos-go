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
