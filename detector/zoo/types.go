package zoo

import (
	"github.com/samuel/go-zookeeper/zk"
)

// Connector Interface to facade zk.Conn type
// since github.com/samuel/go-zookeeper/zk does not provide an interface
// for the zk.Conn object, this allows for mocking and easier testing.
type Connector interface {
	Close()
	Children(string) ([]string, *zk.Stat, error)
	ChildrenW(string) ([]string, *zk.Stat, <-chan zk.Event, error)
	Get(string) ([]byte, *zk.Stat, error)
}

// ChildWatcher interface for handling watcher event
// when zk.EventNodeChildrenChanged.
type ChildWatcher interface {
	childrenChanged(*Client, string)
}

// ChildWatcher adapter function type to facade the interface.
type asChildWatcher func(*Client, string)

func (fn asChildWatcher) childrenChanged(zkc *Client, path string) {
	fn(zkc, path)
}

// ErrorWatcher interface for handling errors.
type ErrorWatcher interface {
	errorOccured(*Client, error)
}

// asErrorWatcher adapter function to facade ErrorWatcher.
type asErrorWatcher func(*Client, error)

func (fn asErrorWatcher) errorOccured(zkc *Client, err error) {
	fn(zkc, err)
}

//Factory is an adapter to trap the creation of zk.Conn instances
//since the official zk API does not expose an interface for zk.Conn.
type Factory interface {
	create() (Connector, <-chan zk.Event, error)
}

type asFactory func() (Connector, <-chan zk.Event, error)

func (f asFactory) create() (Connector, <-chan zk.Event, error) {
	return f()
}
