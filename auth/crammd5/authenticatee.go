package crammd5

import (
	"fmt"
	"sync"

	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"golang.org/x/net/context"
)

var (
	pidLock        sync.Mutex
	pid            int
	supportedMechs map[string]struct{}
)

type statusType int

const (
	READY statusType = iota
	STARTING
	STEPPING
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
	mech       *mechanism
	stepFn     stepFunc
}

func init() {
	supportedMechs = make(map[string]struct{})
	supportedMechs["CRAM-MD5"] = struct{}{}
}

func nextPid() int {
	pidLock.Lock()
	defer pidLock.Unlock()
	pid++
	return pid
}

func Authenticatee(ctx context.Context, pid, client upid.UPID, credential mesos.Credential) <-chan error {

	c := make(chan error, 1)
	f := func() error {
		auth := newAuthenticatee(ctx, client, credential)
		if auth.status == READY {
			defer auth.Stop()
		}
		auth.authenticate(ctx, pid)

		select {
		case <-ctx.Done():
			<-auth.done
			return ctx.Err()
		case <-auth.done:
			return auth.result
		}
	}
	go func() { c <- f() }()
	return c
}

func newAuthenticatee(ctx context.Context, client upid.UPID, credential mesos.Credential) *authenticateeProcess {
	pid := &upid.UPID{ID: fmt.Sprintf("crammd5_authenticatee(%d)", nextPid())}
	transport := messenger.NewMesosMessenger(pid)
	proc := &authenticateeProcess{
		MesosMessenger: transport,
		client:         client,
		credential:     credential,
		status:         READY,
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
			proc._fail(ERROR, err)
			return proc
		}
	}
	if err := proc.Start(); err != nil {
		proc._fail(ERROR, err)
	}
	return proc
}

func (self *authenticateeProcess) _fail(s statusType, err error) {
	self.status = s
	self.result = err
	close(self.done)
}

func (self *authenticateeProcess) authenticate(ctx context.Context, pid upid.UPID) {
	if self.status != READY {
		return
	}
	message := &mesos.AuthenticateMessage{
		Pid: proto.String(self.client.String()),
	}
	if err := self.Send(ctx, &pid, message); err != nil {
		self._fail(ERROR, err)
	} else {
		self.status = STARTING
	}
}

func (self *authenticateeProcess) mechanisms(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	if self.status != STARTING {
		self._fail(ERROR, fmt.Errorf("Unexpected authentication 'mechanisms' received"))
		return
	}
	// TODO(mesos:benh): Store 'from' in order to ensure we only communicate
	// with the same Authenticator.
	msg, ok := pbMsg.(*mesos.AuthenticationMechanismsMessage)
	if !ok {
		self._fail(ERROR, fmt.Errorf("Expected AuthenticationMechanismsMessage, not %T", pbMsg))
		return
	}
	mechanisms := msg.GetMechanisms()
	log.Infof("Received SASL authentication mechanisms: %v", mechanisms)

	selectedMech := ""
	for _, m := range mechanisms {
		if _, ok := supportedMechs[m]; ok {
			selectedMech = m
			break
		}
	}
	if selectedMech == "" {
		self._fail(ERROR, auth.UnsupportedMechanism)
		return
	}

	message := &mesos.AuthenticationStartMessage{
		Mechanism: proto.String(selectedMech),
		// TODO(jdef): Assuming that no data is really needed here since CRAM-MD5 is
		// a single round-trip protocol initiated by the server. Other mechs
		// may need data from an initial step here.
	}

	self.mech = &mechanism{
		username: self.credential.GetPrincipal(),
		secret:   self.credential.GetSecret(),
	}
	self.stepFn = challengeResponse

	if err := self.Send(ctx, from, message); err != nil {
		self._fail(ERROR, err)
	} else {
		self.status = STEPPING
	}
}

func (self *authenticateeProcess) step(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	if self.status != STEPPING {
		self._fail(ERROR, fmt.Errorf("Unexpected authentication 'step' received"))
		return
	}

	log.Infof("Received SASL authentication step")

	msg, ok := pbMsg.(*mesos.AuthenticationStepMessage)
	if !ok {
		self._fail(ERROR, fmt.Errorf("Expected AuthenticationStepMessage, not %T", pbMsg))
		return
	}

	input := msg.GetData()
	fn, output, err := self.stepFn(self.mech, input)

	if err != nil {
		self._fail(ERROR, fmt.Errorf("failed to perform authentication step: %v", err))
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
		self._fail(ERROR, err)
	}
}

func (self *authenticateeProcess) completed(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	if self.status != STEPPING {
		self._fail(ERROR, fmt.Errorf("Unexpected authentication 'completed' received"))
		return
	}

	log.Infof("Authentication success")
	self.status = COMPLETED
	close(self.done)
}

func (self *authenticateeProcess) failed(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	self._fail(FAILED, auth.AuthenticationFailed)
}

func (self *authenticateeProcess) errored(ctx context.Context, from *upid.UPID, pbMsg proto.Message) {
	var err error
	if msg, ok := pbMsg.(*mesos.AuthenticationErrorMessage); !ok {
		err = fmt.Errorf("Expected AuthenticationErrorMessage, not %T", pbMsg)
	} else {
		err = fmt.Errorf("Authentication error: %s", msg.GetError())
	}
	self._fail(ERROR, err)
}
