package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"
)

// ZkConnector Interface to facade zk.Conn type.
// Since github.com/samuel/go-zookeeper does not provide an interface
// for the zk.Conn object, this allows for mocking and easier testing.
type ZkConnector interface {
	Close()
	Children(string) ([]string, *zk.Stat, error)
	ChildrenW(string) ([]string, *zk.Stat, <-chan zk.Event, error)
	Get(string) ([]byte, *zk.Stat, error)
}

// ZkClient interface to interact with and listen for events from
// a Zookeeper server.
type ZkClient interface {
	Connect([]string, string, ZkWatcher) error
	WatchChildren(string) error
	Disconnect()
}

// Interface to handle watcher callback actions from ZkClient
type ZkWatcher interface {
	//Will be called when ZkClient is connected.
	Connected(ZkClient)

	//Called when ZkNode children change
	ChildrenChanged(ZkClient, ZkNode)

	//Called when there's an error in the client.
	Error(error)
}

// ZkNode interface represents a node in Zookeeper path structure.
// Use this interface to retrieve data or other children of that node.
type ZkNode interface {
	//Data retrieves data for the path node.
	Data() ([]byte, error)

	//List returns a sorted (by Path) list of children nodes.
	List() ([]ZkNode, error)

	//Returns the string representation of the path for node.
	String() string
}
