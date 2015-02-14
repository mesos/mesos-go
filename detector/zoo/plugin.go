package zoo

import (
	"github.com/mesos/mesos-go/detector"
)

func init() {
	detector.Register("zk://", detector.PluginFactory(func(spec string) (detector.Master, error) {
		md, err := NewMasterDetector(spec)
		if err == nil {
			md.client.connect()
		}
		return md, err
	}))
}
