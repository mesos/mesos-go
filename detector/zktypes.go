package detector

import (
	"github.com/samuel/go-zookeeper/zk"
)

// ZkConnector Interface to facade zk.Conn type
// since github.com/samuel/go-zookeeper/zk does not provide an interface
// for the zk.Conn object, this allows for mocking and easier testing.
type zkConnector interface {
	Close()
	Children(string) ([]string, *zk.Stat, error)
	ChildrenW(string) ([]string, *zk.Stat, <-chan zk.Event, error)
	Get(string) ([]byte, *zk.Stat, error)
}
