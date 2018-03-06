package detector

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/api/v0/mesosproto"
	util "github.com/mesos/mesos-go/api/v0/mesosutil"
	"github.com/mesos/mesos-go/api/v0/upid"
)

var (
	pluginLock     sync.Mutex
	plugins        = map[string]PluginFactory{}
	EmptySpecError = errors.New("empty master specification")

	defaultFactory = PluginFactory(func(spec string, _ ...Option) (Master, error) {
		if len(spec) == 0 {
			return nil, EmptySpecError
		}
		if strings.Index(spec, "@") < 0 {
			spec = "master@" + spec
		}
		if pid, err := upid.Parse(spec); err == nil {
			return NewStandalone(CreateMasterInfo(pid)), nil
		} else {
			return nil, err
		}
	})
)

type PluginFactory func(string, ...Option) (Master, error)

// associates a plugin implementation with a Master specification prefix.
// packages that provide plugins are expected to invoke this func within
// their init() implementation. schedulers that wish to support plugins may
// anonymously import ("_") a package the auto-registers said plugins.
func Register(prefix string, f PluginFactory) error {
	if prefix == "" {
		return fmt.Errorf("illegal prefix: '%v'", prefix)
	}
	if f == nil {
		return fmt.Errorf("nil plugin factories are not allowed")
	}

	pluginLock.Lock()
	defer pluginLock.Unlock()

	if _, found := plugins[prefix]; found {
		return fmt.Errorf("detection plugin already registered for prefix '%s'", prefix)
	}
	plugins[prefix] = f
	return nil
}

// Create a new detector given the provided specification. Examples are:
//
//   - file://{path_to_local_file}
//   - {ip_address}:{port}
//   - master@{ip_address}:{port}
//   - master({id})@{ip_address}:{port}
//
// Support for the file:// prefix is intentionally hardcoded so that it may
// not be inadvertently overridden by a custom plugin implementation. Custom
// plugins are supported via the Register and MatchingPlugin funcs.
//
// Furthermore it is expected that master detectors returned from this func
// are not yet running and will only begin to spawn requisite background
// processing upon, or some time after, the first invocation of their Detect.
//
func New(spec string, options ...Option) (m Master, err error) {
	if strings.HasPrefix(spec, "file://") {
		var body []byte
		path := spec[7:]
		body, err = ioutil.ReadFile(path)
		if err != nil {
			log.V(1).Infof("failed to read from file at '%s'", path)
		} else {
			m, err = New(string(body), options...)
		}
	} else if f, ok := MatchingPlugin(spec); ok {
		m, err = f(spec, options...)
	} else {
		m, err = defaultFactory(spec, options...)
	}

	return
}

func MatchingPlugin(spec string) (PluginFactory, bool) {
	pluginLock.Lock()
	defer pluginLock.Unlock()

	for prefix, f := range plugins {
		if strings.HasPrefix(spec, prefix) {
			return f, true
		}
	}
	return nil, false
}

// Super-useful utility func that attempts to build a mesos.MasterInfo from a
// upid.UPID specification. An attempt is made to determine the IP address of
// the UPID's Host and any errors during such resolution will result in a nil
// returned result. A nil result is also returned upon errors parsing the Port
// specification of the UPID.
//
// TODO(jdef) make this a func of upid.UPID so that callers can invoke somePid.MasterInfo()?
//
func CreateMasterInfo(pid *upid.UPID) *mesos.MasterInfo {
	if pid == nil {
		return nil
	}

	port, err := strconv.Atoi(pid.Port)
	if err != nil {
		log.Errorf("failed to parse port: %v", err)
		return nil
	}

	ip, err := upid.LookupIP(pid.Host)
	if err != nil {
		log.Error(err)
		return nil
	}

	// MasterInfo.Ip only supports IPv4
	var packedipv4 uint32
	if len(ip) == 4 {
		packedipv4 = binary.BigEndian.Uint32(ip) // network byte order is big-endian
	} else {
		packedipv4 = 0
	}

	mi := util.NewMasterInfoWithAddress(pid.ID, pid.Host, ip.String(), packedipv4, int32(port))
	mi.Pid = proto.String(pid.String())

	return mi
}
