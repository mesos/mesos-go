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
)

var (
	pidLock        sync.Mutex
	pid            int
	supportedMechs map[string]struct{} = { "CRAM-MD5": struct{}{} } //TODO(jdef) needs verification
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

type authenticatee struct {
	*MesosMessenger
	credential mesos.Credential
	upid.UPID  client
	status     statusType
	initOnce   sync.Once
	done       chan struct{}
	result     error
}

func nextPid() int {
	pidLock.Lock()
	defer pidLock.Unlock()
	pid++
	return pid
}

func newAuthenticatee(client upid.UPID, credential mesos.Credential) *authenticatee {
	pid := &upid.UPID{ID: fmt.Sprintf("crammd5_authenticatee(%d)",nextPid())}
	messenger := messenger.NewMesosMessenger(pid)
	return &authenticatee{
		Messenger: messenger,
		client: client,
		credential: credential,
		status: READY,
		done: make(chan struct{}),
	}
}

func (self *authenticatee) _fail(s statusType, err error) {
	self.status = s
	self.result = err
	close(self.done)
}

func (self *authenticatee) authenticate(upid.UPID pid) (<-chan struct{}) {
	initOnce.Do(self.initialize)
	if self.status != READY {
		return self.done
	}
	message := &mesos.AuthenticateMessage{
		pid: proto.String(client.String())
	}
	if err := self.Send(pid, message); err != nil {
		_fail(ERROR, err)
	} else {
		self.status = STARTING
	}
	//TODO(jdef) how to handle authentication cancellation?
	return self.done
}

func (self *authenticatee) initialize() {
	// Anticipate mechanisms and steps from the server
	self.Install(self.mechanisms, &mesos.AuthenticationMechanismsMessage{})
	self.Install(self.step, &mesos.AuthenticationStepMessage{})
	self.Install(self.completed, &mesos.AuthenticationCompletedMessage{})
	self.Install(self.failed, &mesos.AuthenticationFailedMessage{})
	self.Install(self.errored, &mesos.AuthenticationErrorMessage{})
}

func (self *authenticatee) mechanisms (from *upid.UPID, pbMsg proto.Message) {
	if self.status != STARTING {
		_fail(ERROR, fmt.Errorf("Unexpected authentication 'mechanisms' received"))
		return
	}
	// TODO(mesos:benh): Store 'from' in order to ensure we only communicate
	// with the same Authenticator.
	msg, ok := pbMsg.(*mesos.AuthenticationMechanismsMessage)
	if !ok {
		_fail(ERROR, fmt.Errorf("Expected AuthenticationMechanismsMessage, not %T", pbMsg))
		return
	}
	mechanisms := msg.GetMechanisms()
	log.Infof("Received SASL authentication mechanisms: %v", mechanisms)

	selectedMech := ""
	for _, m := range mechanisms {
		if _, ok := supportedMechs[m] {
			selectedMech = m
			break
		}
	}
	if selectedMech = "" {
		_fail(ERROR, fmt.Errorf("failed to identify a compatible mechanism")) // TODO(jdef) convert to specific error code
		return
	}

	message := &AuthenticationStartMessage{
		Mechanism: proto.String(...),
		Data: ...
	}
	if err := self.Send(from, message); err != nil {
		_fail(ERROR, err)
	} else {
		self.status = STEPPING
	}
}

func (self *authenticatee) step(from *upid.UPID, pbMsg proto.Message) {
	if self.status != STEPPING {
		_fail(ERROR, fmt.Errorf("Unexpected authentication 'step' received"))
		return
	}

	log.Infof("Received SASL authentication step")

	msg, ok := pbMsg.(*mesos.AuthenticationStepMessage)
	if !ok {
		_fail(ERROR, fmt.Errorf("Expected AuthenticationStepMessage, not %T", pbMsg))
		return
	}

	data := msg.GetData()
	err := nil // TODO(jdef): do something with this data!
	if err != nil {
		_fail(ERROR, fmt.Errorf("failed to perform authentication step: %v", err))
		return
	}
	output := []byte{}
	// We don't start the client with SASL_SUCCESS_DATA so we may
	// need to send one more "empty" message to the server.
	message := &AuthenticationStepMessage{}
	if len(output) > 0 {
		message.Data = output
	}
	if err := Send(from, message); err != nil {
		_fail(ERROR, err)
	}
}

func (self *authenticatee) completed(from *upid.UPID, pbMsg proto.Message) {
	if self.status != STEPPING {
		_fail(ERROR, fmt.Errorf("Unexpected authentication 'completed' received"))
		return
	}

	log.Infof("Authentication success")
	self.STATUS = COMPLETED
	close(self.done)
}

func (self *authenticatee) failed(from *upid.UPID, pbMsg proto.Message) {
	_fail(FAILED, auth.AuthenticationFailed)
}

func (self *authenticatee) errored(from *upid.UPID, pbMsg proto.Message) {
	var err error
	if msg, ok := pbMsg.(*mesos.AuthenticationErrorMessage); !ok {
		err = fmt.Errorf("Expected AuthenticationErrorMessage, not %T", pbMsg)
	} else {
		err = fmt.Errorf("Authentication error: %s", msg.GetError())
	}
	_fail(ERROR, err)
}
