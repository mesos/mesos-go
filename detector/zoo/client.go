package zoo

import (
	"errors"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

type Client struct {
	conn            Connector
	connFactory     Factory
	hosts           []string
	sessionTimeout  time.Duration
	connTimeout     time.Duration
	connecting      bool
	connected       bool
	reconnCount     int
	reconnMax       int
	reconnDelay     time.Duration
	state           zk.State
	sessionId       int64
	stopCh          chan bool
	rootPath        string
	childrenWatcher ChildWatcher
	errorWatcher    ErrorWatcher
}

func newClient(hosts []string, path string) (*Client, error) {
	zkc := new(Client)
	zkc.hosts = hosts
	zkc.sessionTimeout = time.Second * 60
	zkc.connTimeout = time.Second * 20
	zkc.reconnMax = 5
	zkc.reconnDelay = time.Second * 5
	zkc.rootPath = path

	// TODO: validate  URIs
	return zkc, nil
}

func (zkc *Client) connect() error {
	if zkc.connected || zkc.connecting {
		return nil
	}

	zkc.reconnCount = 0
	zkc.connecting = true
	err := zkc.doConnect()
	if err != nil {
		return err
	}
	zkc.connecting = false
	return nil
}

func (zkc *Client) reconnect() error {
	if zkc.connecting {
		log.V(4).Infoln("Ignoring reconnect, currently connecting.")
		return nil
	}

	// have we reached max count.
	// call connect() to reset count.
	if zkc.reconnCount >= zkc.reconnMax {
		log.V(4).Infoln("Ignoring reconnect, reconnMax reached. Call connect() to reset.")
		return nil
	}
	log.V(4).Infoln("Delaying reconnection for ", zkc.reconnDelay)
	<-time.After(zkc.reconnDelay)

	zkc.conn.Close() // kill any current conn.
	zkc.connected = false
	err := zkc.doConnect()
	if err != nil {
		return err
	}
	zkc.reconnCount++
	return nil
}

func (zkc *Client) doConnect() error {
	var conn Connector
	var ch <-chan zk.Event
	var err error

	// create Connector instance
	if zkc.connFactory == nil {
		var c *zk.Conn
		c, ch, err = zk.Connect(zkc.hosts, zkc.connTimeout)
		conn = Connector(c)
		log.V(4).Infof("Created connection object of type %T\n", conn)
	} else {
		conn, ch, err = zkc.connFactory.create()
		log.V(4).Infof("Created connection object of type %T\n", conn)
	}

	if err != nil {
		return err
	}

	zkc.conn = conn
	zkc.connecting = true
	waitConnCh := make(chan struct{})
	go func(waitCh chan struct{}) {
		for {
			select {
			case e := <-ch:
				if e.Err != nil {
					log.Errorf("Received state error: %s", e.Err.Error())
					if zkc.errorWatcher != nil {
						zkc.errorWatcher.errorOccured(zkc, e.Err)
					}
				}
				switch e.State {
				case zk.StateConnecting:
					log.Infoln("Connecting to zookeeper...")

				case zk.StateConnected:
					zkc.onConnected(e)
					close(waitCh)

				case zk.StateSyncConnected:
					log.Infoln("SyncConnected to zookper server")

				case zk.StateDisconnected:
					zkc.onDisconnected(e)

				case zk.StateExpired:
					log.Infoln("Zookeeper client session expired, disconnecting.")
					//zkc.disconnect()
				}
			}
		}
	}(waitConnCh)

	// wait for connected confirmation
	select {
	case <-waitConnCh:
		zkc.connecting = false
		if !zkc.connected {
			log.V(4).Infof("No connection established within %s\n", zkc.connTimeout.String())
			return zkc.reconnect()
		}
		log.V(2).Infoln("Connection confirmed.")
	case <-time.After(zkc.connTimeout):
		zkc.connecting = false
		return fmt.Errorf("Unable to confirm connection after %v.", time.Second*5)
	}

	return nil
}

func (zkc *Client) disconnect() error {
	if !zkc.connected {
		return nil
	}
	zkc.conn.Close()
	zkc.connecting = false
	zkc.connected = false
	return nil
}

func (zkc *Client) watchChildren(path string) error {
	if !zkc.connected {
		return errors.New("Not connected to server.")
	}
	watchPath := zkc.rootPath
	if path != "" && path != "." {
		watchPath = watchPath + path
	}

	log.V(2).Infoln("Watching children for path", watchPath)
	children, _, ch, err := zkc.conn.ChildrenW(watchPath)
	if err != nil {
		return err
	}

	go func(chList []string) {
		select {
		case e := <-ch:
			if e.Err != nil {
				log.Errorf("Received error while watching path %s: %s", watchPath, e.Err.Error())
				if zkc.errorWatcher != nil {
					zkc.errorWatcher.errorOccured(zkc, e.Err)
				}
			}

			switch e.Type {
			case zk.EventNodeChildrenChanged:
				log.V(2).Infoln("Handling: zk.EventNodeChildrenChanged")
				if zkc.childrenWatcher != nil {
					log.V(2).Infoln("ChildrenWatcher handler found.")
					zkc.childrenWatcher.childrenChanged(zkc, e.Path)
				} else {
					log.Warningln("WARN: No ChildrenWatcher handler found.")
				}
			}
		}
		err := zkc.watchChildren(path)
		if err != nil {
			log.Errorf("Unable to watch children for path %s: %s", path, err.Error())
			if zkc.errorWatcher != nil {
				zkc.errorWatcher.errorOccured(zkc, err)
			}
		}
	}(children)
	return nil
}

func (zkc *Client) onConnected(e zk.Event) {
	if zkc.connected {
		return
	}
	zkc.connected = true
	zkc.state = e.State
	log.Infoln("zk client connected to server.")
}

func (zkc *Client) onDisconnected(e zk.Event) {
	if !zkc.connected {
		return
	}
	zkc.connected = false
	zkc.state = e.State
	log.Infoln("Disconnected from the server, reconnecting...")
	zkc.reconnect() // try to reconnect
}

func (zkc *Client) list(path string) ([]string, error) {
	if !zkc.connected {
		return nil, errors.New("Unable to list children, client not connected.")
	}

	children, _, err := zkc.conn.Children(path)
	if err != nil {
		return nil, err
	}

	return children, nil
}

func (zkc *Client) data(path string) ([]byte, error) {
	if !zkc.connected {
		return nil, errors.New("Unable to retrieve node data, client not connected.")
	}

	data, _, err := zkc.conn.Get(path)
	if err != nil {
		return nil, err
	}

	return data, nil
}
