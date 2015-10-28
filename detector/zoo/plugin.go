package zoo

import (
	"github.com/basho-labs/mesos-go/detector"
)

func init() {
	detector.Register("zk://", detector.PluginFactory(func(spec string) (detector.Master, error) {
		return NewMasterDetector(spec)
	}))
}
