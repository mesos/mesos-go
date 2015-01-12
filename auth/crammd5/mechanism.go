package crammd5

import (
	"crypto/hmac"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"

	log "github.com/golang/glog"
)

var (
	illegalStateErr = errors.New("illegal mechanism state")
)

type mechanism struct {
	username string
	secret   []byte
}

type stepFunc func(m *mechanism, data []byte) (stepFunc, []byte, error)

// algorithm lifted from wikipedia: http://en.wikipedia.org/wiki/CRAM-MD5
// except that the SASL mechanism used by Mesos doesn't leverage base64 encoding
func challengeResponse(m *mechanism, data []byte) (stepFunc, []byte, error) {
	decoded := string(data)
	log.V(4).Infof("challenge(decoded): %s", decoded) // for deep debugging only

	hash := hmac.New(md5.New, m.secret)
	if _, err := io.WriteString(hash, decoded); err != nil {
		return illegalState, nil, err
	}

	codes := hex.EncodeToString(hash.Sum(nil))
	msg := m.username + " " + codes
	return nil, []byte(msg), nil
}

func illegalState(m *mechanism, data []byte) (stepFunc, []byte, error) {
	return illegalState, nil, illegalStateErr
}
