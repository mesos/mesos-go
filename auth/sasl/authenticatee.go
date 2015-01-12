package sasl

import (
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
	*messenger.MesosMessenger
	credential mesos.Credential
	client     upid.UPID
	status     statusType
	done       chan struct{}
	result     error
	mech       mech.Interface
	stepFn     mech.StepFunc
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
		auth := newAuthenticatee(ctx, client, credential)
		auth.authenticate(ctx, pid)

		select {
		case <-ctx.Done():
			err := ctx.Err()
			status := (&auth.status).get()
			for ; status < _TERMINAL; status = (&auth.status).get() {
				if auth.terminate(status, DISCARDED, err) {
					break
				}
			}
			return err
		case <-auth.done:
			return auth.result
		}
	}
	go func() { c <- f() }()
	return c
}

func newAuthenticatee(ctx context.Context, client upid.UPID, credential mesos.Credential) *authenticateeProcess {
	// pid := &upid.UPID{ID: fmt.Sprintf("crammd5_authenticatee(%d)", nextPid())} // TODO(jdef): crammd5 prefix needed?
	pid := &upid.UPID{ID: fmt.Sprintf("sasl_authenticatee(%d)", nextPid())}
	transport := messenger.NewMesosMessenger(pid)
	initialStatus := READY

	proc := &authenticateeProcess{
		MesosMessenger: transport,
		client:         client,
		credential:     credential,
		status:         initialStatus,
		done:           make(chan struct{}),
	}

	type handlerFn func(ctx context.Context, from *upid.UPID, pbMsg proto.Message)
	withContext := func(f handlerFn) messenger.MessageHandler {
		return func(from *upid.UPID, m proto.Message) {
			f(ctx, from, m)
		}
	}
	// Anticipate mechanisms and steps from the server
	handlers := []struct {
		f handlerFn
		m proto.Message
	}{
		{proc.mechanisms, &mesos.AuthenticationMechanismsMessage{}},
		{proc.step, &mesos.AuthenticationStepMessage{}},
		{proc.completed, &mesos.AuthenticationCompletedMessage{}},
		{proc.failed, &mesos.AuthenticationFailedMessage{}},
		{proc.errored, &mesos.AuthenticationErrorMessage{}},
	}
	for _, h := range handlers {
		if err := proc.Install(withContext(h.f), h.m); err != nil {
			proc.terminate(initialStatus, ERROR, err)
			return proc
		}
	}
	if err := proc.Start(); err != nil {
		proc.terminate(initialStatus, ERROR, err)
	} else {
		go func() {
			// stop the authentication transport upon termination of the
			// authenticator process
			select {
			case <-proc.done:
				log.V(2).Infof("stopping authenticator transport: %v", pid)
				proc.Stop()
			}
		}()
	}
	return proc
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
	status := (&self.status).get()
	if status != READY {
		return
	}
	message := &mesos.AuthenticateMessage{
		Pid: proto.String(self.client.String()),
	}
	if err := self.Send(ctx, &pid, message); err != nil {
		self.terminate(status, ERROR, err)
	} else {
		(&self.status).swap(status, STARTING)
	}
}

func (self *authenticateeProcess) mechanisms(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	status := (&self.status).get()
	if status != STARTING {
		self.terminate(status, ERROR, fmt.Errorf("Unexpected authentication 'mechanisms' received")) //TODO(jdef) extract constant error
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

	if err := self.Send(ctx, from, message); err != nil {
		self.terminate(status, ERROR, err)
	} else {
		(&self.status).swap(status, STEPPING)
	}
}

func (self *authenticateeProcess) step(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	status := (&self.status).get()
	if status != STEPPING {
		self.terminate(status, ERROR, fmt.Errorf("Unexpected authentication 'step' received")) // TODO(jdef) extract constant error
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
	if err := self.Send(ctx, from, message); err != nil {
		self.terminate(status, ERROR, err)
	}
}

func (self *authenticateeProcess) completed(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	status := (&self.status).get()
	if status != STEPPING {
		self.terminate(status, ERROR, fmt.Errorf("Unexpected authentication 'completed' received"))
		return
	}

	log.Infof("Authentication success")
	self.terminate(status, COMPLETED, nil)
}

func (self *authenticateeProcess) failed(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	self.terminate((&self.status).get(), FAILED, auth.AuthenticationFailed)
}

func (self *authenticateeProcess) errored(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	var err error
	if msg, ok := pbMsg.(*mesos.AuthenticationErrorMessage); !ok {
		err = fmt.Errorf("Expected AuthenticationErrorMessage, not %T", pbMsg)
	} else {
		err = fmt.Errorf("Authentication error: %s", msg.GetError())
	}
	self.terminate((&self.status).get(), ERROR, err)
}
