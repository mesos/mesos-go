package crammd5

import (
	"crypto/hmac"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"

	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/auth/sasl/callback"
	"github.com/mesos/mesos-go/auth/sasl/mech"
)

var (
	illegalStateErr = errors.New("illegal mechanism state")
	Name            = "CRAM-MD5" // name this mechanism is registered with
)

func init() {
	mech.Register(Name, newInstance)
}

type mechanism struct {
	handler callback.Handler
}

func (m *mechanism) Handler() callback.Handler {
	return m.handler
}

func newInstance(h callback.Handler) (mech.Interface, mech.StepFunc, error) {
	m := &mechanism{
		handler: h,
	}
	fn := func(m mech.Interface, data []byte) (mech.StepFunc, []byte, error) {
		// noop: no initialization needed
		return challengeResponse, nil, nil
	}
	return m, fn, nil
}

// algorithm lifted from wikipedia: http://en.wikipedia.org/wiki/CRAM-MD5
// except that the SASL mechanism used by Mesos doesn't leverage base64 encoding
func challengeResponse(m mech.Interface, data []byte) (mech.StepFunc, []byte, error) {
	decoded := string(data)
	log.V(4).Infof("challenge(decoded): %s", decoded) // for deep debugging only

	username := callback.NewName("", "")
	secret := callback.NewPassword("", false)

	if err := m.Handler().Handle(username, secret); err != nil {
		return illegalState, nil, err
	}
	hash := hmac.New(md5.New, secret.Get())
	if _, err := io.WriteString(hash, decoded); err != nil {
		return illegalState, nil, err
	}

	codes := hex.EncodeToString(hash.Sum(nil))
	msg := username.Get() + " " + codes
	return nil, []byte(msg), nil
}

func illegalState(m mech.Interface, data []byte) (mech.StepFunc, []byte, error) {
	return illegalState, nil, illegalStateErr
}
