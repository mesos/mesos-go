package sasl

import (
	"fmt"
	"testing"

	"github.com/mesos/mesos-go/auth/callback"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"
)

//TODO(jdef) move MockTransporter to the messenger package
type MockTransporter struct {
	mock.Mock
}

func (m *MockTransporter) Send(ctx context.Context, msg *messenger.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}
func (m *MockTransporter) Listen() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockTransporter) Recv() *messenger.Message {
	args := m.Called()
	return args.Get(0).(*messenger.Message)
}
func (m *MockTransporter) Inject(ctx context.Context, msg *messenger.Message) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}
func (m *MockTransporter) Install(messageName string) {
	m.Called(messageName)
}
func (m *MockTransporter) Start() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockTransporter) Stop() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockTransporter) UPID() *upid.UPID {
	args := m.Called()
	return args.Get(0).(*upid.UPID)
}

func TestAuthticatee_validLogin(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()
	client := upid.UPID{
		ID:   "someFramework",
		Host: "b.net",
		Port: "789",
	}
	handler := callback.HandlerFunc(func(cb ...callback.Interface) error {
		for _, c := range cb {
			switch c := c.(type) {
			case *callback.Name:
				c.Set("foo")
			case *callback.Password:
				c.Set([]byte("bar"))
			case *callback.Interprocess:
				c.Set(upid.UPID{ID: "serv", Host: "a.com", Port: "123"}, client)
			default:
				return &callback.Unsupported{c}
			}
		}
		return nil
	})

	// TODO(jdef) there's too much duplication between this and sasl.Authenticatee

	ip := callback.NewInterprocess()
	assert.Nil(handler.Handle(ip))

	tpid := &upid.UPID{ID: fmt.Sprintf("mock_authenticatee(%d)", nextPid())}
	transport := &MockTransporter{}
	transport.On("Install", "mesos.internal.AuthenticationMechanismsMessage").Return(nil)
	transport.On("Install", "mesos.internal.AuthenticationStepMessage").Return(nil)
	transport.On("Install", "mesos.internal.AuthenticationCompletedMessage").Return(nil)
	transport.On("Install", "mesos.internal.AuthenticationFailedMessage").Return(nil)
	transport.On("Install", "mesos.internal.AuthenticationErrorMessage").Return(nil)
	transport.On("Listen").Return(nil)
	transport.On("UPID").Return(tpid)
	transport.On("Start").Return(nil)
	transport.On("Stop").Return(nil)

	//TODO(jdef) set transport expectations

	config := &authenticateeConfig{
		client:    ip.Client(),
		handler:   handler,
		transport: messenger.New(tpid, transport),
	}
	ctx, auth := newAuthenticatee(ctx, config)
	auth.authenticate(ctx, ip.Server())

	select {
	case <-ctx.Done():
		t.Fatal(auth.discard(ctx))
	case <-auth.done:
		assert.Nil(auth.err)
	}
	transport.AssertExpectations(t)
}
