package executor

import (
	"reflect"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"github.com/mesosphere/testify/mock"
)

type message struct {
	from *upid.UPID
	msg  proto.Message
}

// MockedMessenger is a messenger that returns error on every operation.
type MockedMessenger struct {
	mock.Mock
	messageQueue chan *message
	handlers     map[string]messenger.MessageHandler
	stop         chan struct{}
}

// NewMockedMessenger returns a mocked messenger used for testing.
func NewMockedMessenger() *MockedMessenger {
	return &MockedMessenger{
		messageQueue: make(chan *message, 1),
		handlers:     make(map[string]messenger.MessageHandler),
		stop:         make(chan struct{}),
	}
}

// Install is a mocked implementation.
func (m *MockedMessenger) Install(handler messenger.MessageHandler, msg proto.Message) error {
	m.handlers[reflect.TypeOf(msg).Elem().Name()] = handler
	return m.Called().Error(0)
}

// Send is a mocked implementation.
func (m *MockedMessenger) Send(upid *upid.UPID, msg proto.Message) error {
	return m.Called().Error(0)
}

// Start is a mocked implementation.
func (m *MockedMessenger) Start() error {
	go m.recvLoop()
	return m.Called().Error(0)
}

// Stop is a mocked implementation.
func (m *MockedMessenger) Stop() {
	close(m.stop)
	m.Called()
}

// UPID is a mocked implementation.
func (m *MockedMessenger) UPID() *upid.UPID {
	return m.Called().Get(0).(*upid.UPID)
}

func (m *MockedMessenger) recvLoop() {
	for {
		select {
		case <-m.stop:
			return
		case msg := <-m.messageQueue:
			name := reflect.TypeOf(msg.msg).Elem().Name()
			m.handlers[name](msg.from, msg.msg)
		}
	}
}

// Recv receives a upid and a message, it will dispatch the message to its handler
// with the upid. This is for testing.
func (m *MockedMessenger) Recv(from *upid.UPID, msg proto.Message) {
	m.messageQueue <- &message{from, msg}
}
