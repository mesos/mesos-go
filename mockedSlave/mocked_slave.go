package mockedSlave

import (
	"code.google.com/p/gogoprotobuf/proto"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"github.com/mesosphere/testify/assert"
	"github.com/mesosphere/testify/mock"
)

type testing interface {
	Fatalf(format string, args ...interface{})
}

// MockedSlave is used for testing the executor driver.
type MockedSlave struct {
	Mock      *mock.Mock
	messenger messenger.Messenger
}

// NewMockedSlave returns a mocked slave.
func NewMockedSlave(t testing, upid *upid.UPID) *MockedSlave {
	s := new(MockedSlave)
	s.messenger = messenger.NewMesosMessenger(upid)
	assert.NoError(t, s.messenger.Install(s.RegisterExecutor, &mesosproto.RegisterExecutorMessage{}))
	assert.NoError(t, s.messenger.Install(s.ReregisterExecutor, &mesosproto.ReregisterExecutorMessage{}))
	assert.NoError(t, s.messenger.Install(s.StatusUpdate, &mesosproto.StatusUpdateMessage{}))
	assert.NoError(t, s.messenger.Install(s.FrameworkMessage, &mesosproto.FrameworkToExecutorMessage{}))
	assert.NoError(t, s.messenger.Start())
	return s
}

// Refresh cleans the mock object in the mocked slave.
func (s *MockedSlave) Refresh() {
	s.Mock = new(mock.Mock)
}

// Stop stops the mocked slave.
func (s *MockedSlave) Stop() {
	s.messenger.Stop()
}

// RegisterExecutor is the handler for RegisterExecutorMessage.
func (s *MockedSlave) RegisterExecutor(upid *upid.UPID, msg proto.Message) {
	s.Mock.Called(upid, msg)
}

// ReregisterExecutor is the handler for ReregisterExecutorMessage.
func (s *MockedSlave) ReregisterExecutor(upid *upid.UPID, msg proto.Message) {
	s.Mock.Called(upid, msg)
}

// StatusUpdate is the handler for StatusUpdateMessage.
func (s *MockedSlave) StatusUpdate(upid *upid.UPID, msg proto.Message) {
	s.Mock.Called(upid, msg)
}

// FrameworkMessage is the handler for FrameworkMessage
func (s *MockedSlave) FrameworkMessage(upid *upid.UPID, msg proto.Message) {
	s.Mock.Called(upid, msg)
}
