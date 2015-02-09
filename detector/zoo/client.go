package zoo

import (
	"errors"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	defaultConnectTimeout    = 20 * time.Second
	defaultSessionTimeout    = 60 * time.Second
	defaultReconnectTimeout  = 5 * time.Second
	defaultReconnectAttempts = 5
	currentPath              = "."
)

type Client struct {
	conn            Connector
	connFactory     Factory
	connTimeout     time.Duration
	connecting      bool
	connected       bool
	reconnCount     int
	reconnMax       int
	reconnDelay     time.Duration
	rootPath        string
	childrenWatcher ChildWatcher
	errorWatcher    ErrorWatcher
}

func newClient(hosts []string, path string) (*Client, error) {
	connTimeout := defaultConnectTimeout
	zkc := &Client{
		connTimeout: connTimeout,
		reconnMax:   defaultReconnectAttempts,
		reconnDelay: defaultReconnectTimeout,
		rootPath:    path,
		connFactory: asFactory(func() (Connector, <-chan zk.Event, error) {
			return zk.Connect(hosts, defaultSessionTimeout)
		}),
		childrenWatcher: asChildWatcher(func(*Client, string) {}),
		errorWatcher:    asErrorWatcher(func(*Client, error) {}),
	}
	// TODO(vlad): validate  URIs
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
	// create Connector instance
	conn, sessionEvents, err := zkc.connFactory.create()
	if err != nil {
		return err
	} else {
		log.V(4).Infof("Created connection object of type %T\n", conn)
	}

	zkc.conn = conn
	zkc.connecting = true
	connected := make(chan struct{})
	go zkc.monitorSession(sessionEvents, connected)

	// wait for connected confirmation
	select {
	case <-connected:
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

// monitor a zookeeper session event channel, closes the 'connected' channel once
// a zookeeper connection has been established. errors are forwarded to the client's
// errorWatcher. disconnected events are forwarded to client.onDisconnected.
func (zkc *Client) monitorSession(sessionEvents <-chan zk.Event, connected chan struct{}) {
	for e := range sessionEvents {
		if e.Err != nil {
			log.Errorf("Received state error: %s", e.Err.Error())
			zkc.errorWatcher.errorOccured(zkc, e.Err)
		}
		switch e.State {
		case zk.StateConnecting:
			log.Infoln("Connecting to zookeeper...")

		case zk.StateConnected:
			zkc.onConnected(e)
			close(connected)

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

func (zkc *Client) disconnect() error {
	if !zkc.connected {
		return nil
	}
	zkc.conn.Close()
	zkc.connecting = false
	zkc.connected = false
	return nil
}

// watch the child nodes for changes, at the specified path.
// callers that specify a path of `currentPath` will watch the currently set rootPath,
// otherwise the watchedPath is calculated as rootPath+path.
// this func spawns a go routine to actually do the watching, and so returns immediately.
// in the absense of errors a signalling channel is returned that will close
// upon the termination of the watch (e.g. due to disconnection).
func (zkc *Client) watchChildren(path string) (<-chan struct{}, error) {
	if !zkc.connected {
		return nil, errors.New("Not connected to server.")
	}

	watchPath := zkc.rootPath
	if path != "" && path != currentPath {
		watchPath = watchPath + path
	}

	log.V(2).Infoln("Watching children for path", watchPath)
	_, _, ch, err := zkc.conn.ChildrenW(watchPath)
	if err != nil {
		return nil, err
	}

	watchEnded := make(chan struct{})
	go func() {
		for {
			for e := range ch {
				if e.Err != nil {
					log.Errorf("Received error while watching path %s: %s", watchPath, e.Err.Error())
					zkc.errorWatcher.errorOccured(zkc, e.Err)
				}

				switch e.Type {
				case zk.EventNodeChildrenChanged:
					log.V(2).Infoln("Handling: zk.EventNodeChildrenChanged")
					zkc.childrenWatcher.childrenChanged(zkc, e.Path)
				}
			}
			//TODO(jdef) sleep here to avoid potentially hogging CPU in loop spins?
			if !zkc.connected {
				log.V(1).Info("no longer connected to server.")
				close(watchEnded)
				return
			}
			_, _, ch, err = zkc.conn.ChildrenW(watchPath)
			if err != nil {
				log.Errorf("Unable to watch children for path %s: %s", path, err.Error())
				zkc.errorWatcher.errorOccured(zkc, err)
			}
			log.V(2).Infoln("rewatching children for path", watchPath)
		}
	}()
	return watchEnded, nil
}

func (zkc *Client) onConnected(e zk.Event) {
	if zkc.connected {
		return
	}
	zkc.connected = true
	log.Infoln("zk client connected to server.")
}

func (zkc *Client) onDisconnected(e zk.Event) {
	if !zkc.connected {
		return
	}
	zkc.connected = false
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
