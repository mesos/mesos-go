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

func NewMockedMessenger() *MockedMessenger {
	return &MockedMessenger{
		messageQueue: make(chan *message, 1),
		handlers:     make(map[string]messenger.MessageHandler),
		stop:         make(chan struct{}),
	}
}

func (m *MockedMessenger) Install(handler messenger.MessageHandler, msg proto.Message) error {
	m.handlers[reflect.TypeOf(msg).Elem().Name()] = handler
	return m.Called().Error(0)
}

func (m *MockedMessenger) Send(upid *upid.UPID, msg proto.Message) error {
	return m.Called().Error(0)
}

func (m *MockedMessenger) Start() error {
	go m.recvLoop()
	return m.Called().Error(0)
}

func (m *MockedMessenger) Stop() {
	close(m.stop)
	m.Called()
}

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

func (m *MockedMessenger) Recv(from *upid.UPID, msg proto.Message) {
	m.messageQueue <- &message{from, msg}
}
