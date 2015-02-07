package detector

import (
	"io/ioutil"
	"net"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/upid"
	"github.com/mesos/mesos-go/detector/zoo"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

//TODO(jdef) import cycle not allowed between detector/zoo and detector, so:
//(1) implement plugin pattern: zoo/plugin.go will Register a factory func with a prefix
//(2) New() will check the registered plugins, if not found will create a NewStandalone

func New(spec string) (m Master, err error) {
	if spec == "" {
		m = NewStandalone(nil)
	} else if strings.HasPrefix(spec, "zk://") {
		m, err = zoo.NewMasterDetector(spec)
	} else if strings.HasPrefix(spec, "file://") {
		var body []byte
		path := spec[7:]
		body, err = ioutil.ReadFile(path)
		if err != nil {
			log.V(1).Infof("failed to read from file at '%s'", path)
		} else {
			m, err = New(string(body))
		}
	} else if strings.HasPrefix("master@", spec) {
		var pid *upid.UPID
		if pid, err = upid.Parse(spec); err == nil {
			m = NewStandalone(createMasterInfo(pid))
		}
	} else {
		var pid *upid.UPID
		if pid, err = upid.Parse("master@"+spec); err == nil {
			m = NewStandalone(createMasterInfo(pid))
		}
	}
	return
}

func createMasterInfo(pid *upid.UPID) *mesos.MasterInfo {
	port, err := strconv.Atoi(pid.Port)
	if err != nil {
		log.Errorf("failed to parse port: %v", err)
		return nil
	}
	ip := net.ParseIP(pid.Host)
	if ip == nil {
		log.Errorf("failed to parse IP address: '%v'", pid.Host)
		return nil
	}
	ip = ip.To4()
	if ip == nil {
		log.Errorf("IP address is not IPv4: %v", pid.Host)
		return nil
	}
	merged := (uint32(ip[0]) << 24) | (uint32(ip[1]) << 16) | (uint32(ip[2]) << 8) | (0xF & uint32(ip[3]))
	return util.NewMasterInfo(pid.ID, merged, uint32(port))
}
