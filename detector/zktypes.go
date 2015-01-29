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

// zkChildrenWatcher interface for handling watcher event
// when zk.EventNodeChildrenChanged.
type zkChildrenWatcher interface {
	childrenChanged(*zkClient, string)
}

// zkChildrenWatcherFunk adapter function type to facade the interface.
type zkChildrenWatcherFunc func(*zkClient, string)

func (fn zkChildrenWatcherFunc) childrenChanged(zkc *zkClient, path string) {
	fn(zkc, path)
}

// zkErrorWatcher interface for handling errors.
type zkErrorWatcher interface {
	errorOccured(*zkClient, error)
}

// zkErrorWatcherFunc adapter function to facade zkErrorWatcher.
type zkErrorWatcherFunc func(*zkClient, error)

func (fn zkErrorWatcherFunc) errorOccured(zkc *zkClient, err error) {
	fn(zkc, err)
}

//zkConnFactory is an adapter to trap the creation of zk.Conn instances
//since the official zk API does not expose an interface for zk.Conn.
type zkConnFactory interface {
	create() (zkConnector, <-chan zk.Event, error)
}

type zkConnFactoryFunc func() (zkConnector, <-chan zk.Event, error)

func (f zkConnFactoryFunc) create() (zkConnector, <-chan zk.Event, error) {
	return f()
}
