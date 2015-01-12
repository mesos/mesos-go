package crammd5

import (
	"crypto/hmac"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"

	log "github.com/golang/glog"
)

type mechanism struct {
	username string
	secret []byte
}

type stepFunc func(m *mechanism, data []byte) (stepFunc, []byte, error)

func challengeResponse(m *mechanism, data []byte) (stepFunc, []byte, error) {
	sdata := string(data)
	log.V(2).Info("challenge: %s", sdata)

	decoded, err := base64.StdEncoding.DecodeString(sdata)
	if err != nil {
		return noop, nil, err
	}
	log.V(2).Info("challenge(decoded): %s", sdata)

	// hash_hmac_md5(decoded, secret)
	hash := hmac.New(md5.New, m.secret)
	_, err = hash.Write(decoded)
	if err != nil {
		return noop, nil, err
	}
	mac := hash.Sum(nil)

	// hexcodes := serialize(mac)
	codes := hex.EncodeToString(mac)

	msg := base64.StdEncoding.EncodeToString([]byte(m.username + " " + codes))
	return nil, []byte(msg), nil
}

func noop(m *mechanism, data []byte) (stepFunc, []byte, error) {
	sdata := string(data)
	log.V(2).Info("noop: %s", sdata)
	return noop,nil,nil
}
