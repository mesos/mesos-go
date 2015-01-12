package sasl

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth"
	"github.com/mesos/mesos-go/auth/sasl/mech"
	helper "github.com/mesos/mesos-go/auth/sasl/mesos"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"golang.org/x/net/context"
)

var (
	pidLock sync.Mutex
	pid     int

	UnexpectedAuthenticationMechanisms = errors.New("Unexpected authentication 'mechanisms' received")
	UnexpectedAuthenticationStep       = errors.New("Unexpected authentication 'step' received")
)

type statusType int32

const (
	READY statusType = iota
	STARTING
	STEPPING
	_TERMINAL // meta status, should never be assigned: all status types following are "terminal"
	COMPLETED
	FAILED
	ERROR
	DISCARDED
)

type authenticateeProcess struct {
	transport  messenger.Messenger
	credential mesos.Credential
	client     upid.UPID
	status     statusType
	done       chan struct{}
	result     error
	mech       mech.Interface
	stepFn     mech.StepFunc
}

type authenticateeConfig struct {
	client     upid.UPID           // pid of the client we're attempting to authenticate
	credential mesos.Credential    // user-specified credentials
	transport  messenger.Messenger // mesos communications transport
}

func nextPid() int {
	pidLock.Lock()
	defer pidLock.Unlock()
	pid++
	return pid
}

func (s *statusType) get() statusType {
	return statusType(atomic.LoadInt32((*int32)(s)))
}

func (s *statusType) swap(old, new statusType) bool {
	return old != new && atomic.CompareAndSwapInt32((*int32)(s), int32(old), int32(new))
}

func Authenticatee(ctx context.Context, pid, client upid.UPID, credential mesos.Credential) <-chan error {

	c := make(chan error, 1)
	f := func() error {
		config := newConfig(client, credential)
		ctx, auth := newAuthenticatee(ctx, config)
		auth.authenticate(ctx, pid)

		select {
		case <-ctx.Done():
			return auth.discard(ctx)
		case <-auth.done:
			return auth.result
		}
	}
	go func() { c <- f() }()
	return c
}

func newConfig(client upid.UPID, credential mesos.Credential) *authenticateeConfig {
	tpid := &upid.UPID{ID: fmt.Sprintf("sasl_authenticatee(%d)", nextPid())}
	return &authenticateeConfig{
		client:     client,
		credential: credential,
		transport:  messenger.NewMesosMessenger(tpid),
	}
}

// Terminate the authentication process upon context cancellation;
// only to be called if/when ctx.Done() has been signalled.
func (self *authenticateeProcess) discard(ctx context.Context) error {
	err := ctx.Err()
	status := StatusFrom(ctx)
	for ; status < _TERMINAL; status = (&self.status).get() {
		if self.terminate(status, DISCARDED, err) {
			break
		}
	}
	return err
}

func newAuthenticatee(ctx context.Context, config *authenticateeConfig) (context.Context, *authenticateeProcess) {
	initialStatus := READY
	proc := &authenticateeProcess{
		transport:  config.transport,
		client:     config.client,
		credential: config.credential,
		status:     initialStatus,
		done:       make(chan struct{}),
	}
	ctx = WithStatus(ctx, initialStatus)
	err := proc.installHandlers(ctx)
	if err == nil {
		err = proc.startTransport()
	}
	if err != nil {
		proc.terminate(initialStatus, ERROR, err)
	}
	return ctx, proc
}

func (self *authenticateeProcess) startTransport() error {
	if err := self.transport.Start(); err != nil {
		return err
	} else {
		go func() {
			// stop the authentication transport upon termination of the
			// authenticator process
			select {
			case <-self.done:
				log.V(2).Infof("stopping authenticator transport: %v", self.transport.UPID())
				self.transport.Stop()
			}
		}()
	}
	return nil
}

// returns true when handlers are installed without error, otherwise terminates the
// authentication process.
func (self *authenticateeProcess) installHandlers(ctx context.Context) error {

	type handlerFn func(ctx context.Context, from *upid.UPID, pbMsg proto.Message)

	withContext := func(f handlerFn) messenger.MessageHandler {
		return func(from *upid.UPID, m proto.Message) {
			status := (&self.status).get()
			f(WithStatus(ctx, status), from, m)
		}
	}

	// Anticipate mechanisms and steps from the server
	handlers := []struct {
		f handlerFn
		m proto.Message
	}{
		{self.mechanisms, &mesos.AuthenticationMechanismsMessage{}},
		{self.step, &mesos.AuthenticationStepMessage{}},
		{self.completed, &mesos.AuthenticationCompletedMessage{}},
		{self.failed, &mesos.AuthenticationFailedMessage{}},
		{self.errored, &mesos.AuthenticationErrorMessage{}},
	}
	for _, h := range handlers {
		if err := self.transport.Install(withContext(h.f), h.m); err != nil {
			return err
		}
	}
	return nil
}

// return true if the authentication status was updated (if true, self.done will have been closed)
func (self *authenticateeProcess) terminate(old, new statusType, err error) bool {
	if (&self.status).swap(old, new) {
		self.result = err
		close(self.done)
		return true
	}
	return false
}

func (self *authenticateeProcess) authenticate(ctx context.Context, pid upid.UPID) {
	status := StatusFrom(ctx)
	if status != READY {
		return
	}
	message := &mesos.AuthenticateMessage{
		Pid: proto.String(self.client.String()),
	}
	if err := self.transport.Send(ctx, &pid, message); err != nil {
		self.terminate(status, ERROR, err)
	} else {
		(&self.status).swap(status, STARTING)
	}
}

func (self *authenticateeProcess) mechanisms(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	status := StatusFrom(ctx)
	if status != STARTING {
		self.terminate(status, ERROR, UnexpectedAuthenticationMechanisms)
		return
	}
	// TODO(mesos:benh): Store 'from' in order to ensure we only communicate
	// with the same Authenticator.
	msg, ok := pbMsg.(*mesos.AuthenticationMechanismsMessage)
	if !ok {
		self.terminate(status, ERROR, fmt.Errorf("Expected AuthenticationMechanismsMessage, not %T", pbMsg))
		return
	}

	mechanisms := msg.GetMechanisms()
	log.Infof("Received SASL authentication mechanisms: %v", mechanisms)

	selectedMech, factory := mech.SelectSupported(mechanisms)
	if selectedMech == "" {
		self.terminate(status, ERROR, auth.UnsupportedMechanism)
		return
	}

	if m, f, err := factory(&helper.CredentialHandler{&self.credential}); err != nil {
		self.terminate(status, ERROR, err)
		return
	} else {
		self.mech = m
		self.stepFn = f
	}

	// execute initialization step...
	nextf, data, err := self.stepFn(self.mech, nil)
	if err != nil {
		self.terminate(status, ERROR, err)
		return
	} else {
		self.stepFn = nextf
	}

	message := &mesos.AuthenticationStartMessage{
		Mechanism: proto.String(selectedMech),
		Data:      proto.String(string(data)), // may be nil, depends on init step
	}

	if err := self.transport.Send(ctx, from, message); err != nil {
		self.terminate(status, ERROR, err)
	} else {
		(&self.status).swap(status, STEPPING)
	}
}

func (self *authenticateeProcess) step(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	status := StatusFrom(ctx)
	if status != STEPPING {
		self.terminate(status, ERROR, UnexpectedAuthenticationStep)
		return
	}

	log.Infof("Received SASL authentication step")

	msg, ok := pbMsg.(*mesos.AuthenticationStepMessage)
	if !ok {
		self.terminate(status, ERROR, fmt.Errorf("Expected AuthenticationStepMessage, not %T", pbMsg))
		return
	}

	input := msg.GetData()
	fn, output, err := self.stepFn(self.mech, input)

	if err != nil {
		self.terminate(status, ERROR, fmt.Errorf("failed to perform authentication step: %v", err))
		return
	}
	self.stepFn = fn

	// We don't start the client with SASL_SUCCESS_DATA so we may
	// need to send one more "empty" message to the server.
	message := &mesos.AuthenticationStepMessage{}
	if len(output) > 0 {
		message.Data = output
	}
	if err := self.transport.Send(ctx, from, message); err != nil {
		self.terminate(status, ERROR, err)
	}
}

func (self *authenticateeProcess) completed(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	status := StatusFrom(ctx)
	if status != STEPPING {
		self.terminate(status, ERROR, fmt.Errorf("Unexpected authentication 'completed' received"))
		return
	}

	log.Infof("Authentication success")
	self.terminate(status, COMPLETED, nil)
}

func (self *authenticateeProcess) failed(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	status := StatusFrom(ctx)
	self.terminate(status, FAILED, auth.AuthenticationFailed)
}

func (self *authenticateeProcess) errored(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	var err error
	if msg, ok := pbMsg.(*mesos.AuthenticationErrorMessage); !ok {
		err = fmt.Errorf("Expected AuthenticationErrorMessage, not %T", pbMsg)
	} else {
		err = fmt.Errorf("Authentication error: %s", msg.GetError())
	}
	status := StatusFrom(ctx)
	self.terminate(status, ERROR, err)
}
