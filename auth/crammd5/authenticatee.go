package crammd5

import (
	"fmt"
	"sync"

	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
        "github.com/mesos/mesos-go/auth"
        "github.com/mesos/mesos-go/messenger"
        "github.com/mesos/mesos-go/upid"
        mesos "github.com/mesos/mesos-go/mesosproto"
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
	initOnce   sync.Once
	done       chan struct{}
	result     error
	ctx        context.Context
	cancel     context.CancelFunc
	mech       *mechanism
	stepFn     stepFunc
}

func init() {
	supportedMechs["CRAM-MD5"] = struct{}{} //TODO(jdef) needs verification
}

func nextPid() int {
	pidLock.Lock()
	defer pidLock.Unlock()
	pid++
	return pid
}

func Authenticatee(pid, client upid.UPID, credential mesos.Credential) <- chan error {

	c := make(chan error, 1)
	f := func() error {
		auth := newAuthenticatee(client, credential)
		defer auth.cancel()

		auth.authenticate(pid)
		select {
		case <- auth.ctx.Done():
			<- auth.done
			return auth.ctx.Err()
		case <- auth.done:
			return auth.result
		}
	}
	go func() { c <- f() }()
	return c
}

func newAuthenticatee(client upid.UPID, credential mesos.Credential) *authenticateeProcess {
	pid := &upid.UPID{ID: fmt.Sprintf("crammd5_authenticatee(%d)",nextPid())}
	messenger := messenger.NewMesosMessenger(pid)
	ctx, cancel := context.WithCancel(context.Background()) // TODO(jdef) support timeout
	return &authenticateeProcess{
		MesosMessenger: messenger,
		client: client,
		credential: credential,
		status: READY,
		done: make(chan struct{}),
		ctx: ctx,
		cancel: cancel,
	}
}

func (self *authenticateeProcess) _fail(s statusType, err error) {
	self.status = s
	self.result = err
	close(self.done)
}

func (self *authenticateeProcess) send(upid *upid.UPID, msg proto.Message) error {
	return self.Send(self.ctx, upid, msg)
}

func (self *authenticateeProcess) authenticate(pid upid.UPID) {
	self.initOnce.Do(self.initialize)
	if self.status != READY {
		return
	}
	message := &mesos.AuthenticateMessage{
		Pid: proto.String(self.client.String()),
	}
	if err := self.send(&pid, message); err != nil {
		self._fail(ERROR, err)
	} else {
		self.status = STARTING
	}
}

func (self *authenticateeProcess) initialize() {
	// Anticipate mechanisms and steps from the server
	self.Install(self.mechanisms, &mesos.AuthenticationMechanismsMessage{})
	self.Install(self.step, &mesos.AuthenticationStepMessage{})
	self.Install(self.completed, &mesos.AuthenticationCompletedMessage{})
	self.Install(self.failed, &mesos.AuthenticationFailedMessage{})
	self.Install(self.errored, &mesos.AuthenticationErrorMessage{})
}

func (self *authenticateeProcess) mechanisms (from *upid.UPID, pbMsg proto.Message) {
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
		self._fail(ERROR, fmt.Errorf("failed to identify a compatible mechanism")) // TODO(jdef) convert to specific error code
		return
	}

	message := &mesos.AuthenticationStartMessage{
		Mechanism: proto.String(selectedMech),
		// TODO(jdef) assuming that no data is really needed here since CRAM-MD5 is
		// a single round-trip protocol initiated by the server
	}

	self.mech = &mechanism{
		username: self.credential.GetPrincipal(),
		secret: self.credential.GetSecret(),
	}
	self.stepFn = challengeResponse

	if err := self.send(from, message); err != nil {
		self._fail(ERROR, err)
	} else {
		self.status = STEPPING
	}
}

func (self *authenticateeProcess) step(from *upid.UPID, pbMsg proto.Message) {
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
	if err := self.send(from, message); err != nil {
		self._fail(ERROR, err)
	}
}

func (self *authenticateeProcess) completed(from *upid.UPID, pbMsg proto.Message) {
	if self.status != STEPPING {
		self._fail(ERROR, fmt.Errorf("Unexpected authentication 'completed' received"))
		return
	}

	log.Infof("Authentication success")
	self.status = COMPLETED
	close(self.done)
}

func (self *authenticateeProcess) failed(from *upid.UPID, pbMsg proto.Message) {
	self._fail(FAILED, auth.AuthenticationFailed)
}

func (self *authenticateeProcess) errored(from *upid.UPID, pbMsg proto.Message) {
	var err error
	if msg, ok := pbMsg.(*mesos.AuthenticationErrorMessage); !ok {
		err = fmt.Errorf("Expected AuthenticationErrorMessage, not %T", pbMsg)
	} else {
		err = fmt.Errorf("Authentication error: %s", msg.GetError())
	}
	self._fail(ERROR, err)
}
