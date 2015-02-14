package detector

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/upid"
)

var (
	pluginLock     sync.Mutex
	plugins        = map[string]PluginFactory{}
	EmptySpecError = errors.New("empty master specification")

	defaultFactory = PluginFactory(func(spec string) (Master, error) {
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

type PluginFactory func(string) (Master, error)

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

func New(spec string) (m Master, err error) {
	if strings.HasPrefix(spec, "file://") {
		var body []byte
		path := spec[7:]
		body, err = ioutil.ReadFile(path)
		if err != nil {
			log.V(1).Infof("failed to read from file at '%s'", path)
		} else {
			m, err = New(string(body))
		}
	} else if f, ok := MatchingPlugin(spec); ok {
		m, err = f(spec)
	} else {
		m, err = defaultFactory(spec)
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

func CreateMasterInfo(pid *upid.UPID) *mesos.MasterInfo {
	port, err := strconv.Atoi(pid.Port)
	if err != nil {
		log.Errorf("failed to parse port: %v", err)
		return nil
	}
	//TODO(jdef) attempt to resolve host to IP address
	var ipv4 net.IP
	if addrs, err := net.LookupIP(pid.Host); err == nil {
		for _, ip := range addrs {
			if ip = ip.To4(); ip != nil {
				ipv4 = ip
				break
			}
		}
		if ipv4 == nil {
			log.Errorf("host does not resolve to an IPv4 address: %v", pid.Host)
			return nil
		}
	} else {
		log.Errorf("failed to lookup IPs for host '%v': %v", pid.Host, err)
		return nil
	}
	packedip := binary.BigEndian.Uint32(ipv4) // network byte order is big-endian
	mi := util.NewMasterInfo(pid.ID, packedip, uint32(port))
	mi.Pid = proto.String(pid.String())
	if pid.Host != "" {
		mi.Hostname = proto.String(pid.Host)
	}
	return mi
}
