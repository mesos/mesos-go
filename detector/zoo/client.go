package zoo

import (
	"errors"
	"fmt"
	"sync/atomic"
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

type stateType int32

const (
	disconnectedState stateType = iota
	connectionRequestedState
	connectionAttemptState
	connectedState
)

type Client struct {
	conn            Connector
	connFactory     Factory // must never be nil
	connTimeout     time.Duration
	state           stateType
	reconnCount     int
	reconnMax       int
	reconnDelay     time.Duration
	rootPath        string
	childrenWatcher ChildWatcher // must never be nil
	errorWatcher    ErrorWatcher // must never be nil
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

// return true only if the client's state was changed from `from` to `to`
func (zkc *Client) stateChange(from, to stateType) bool {
	return atomic.CompareAndSwapInt32((*int32)(&zkc.state), int32(from), int32(to))
}

func (zkc *Client) connect() (err error) {
	if zkc.stateChange(disconnectedState, connectionRequestedState) {
		zkc.reconnCount = 0
		err = zkc.doConnect()
	}
	return
}

// attempt to reconnect to zookeeper. will ignore attempts to reconnect
// if not disconnected, or else if reconnCount has reached or exceeded reconnMax.
// if reconnection is attempted then this func will block for at least reconnDelay
// before actually attempting to connect to zookeeper.
func (zkc *Client) reconnect() error {
	if !zkc.stateChange(disconnectedState, connectionRequestedState) {
		log.V(4).Infoln("Ignoring reconnect, currently connected/connecting.")
		return nil
	}

	// have we reached max count. call connect() to reset count.
	if zkc.reconnCount >= zkc.reconnMax {
		log.V(4).Infoln("Ignoring reconnect, reconnMax reached. Call connect() to reset.")
		return nil
	}

	defer func() { zkc.reconnCount++ }()

	log.V(4).Infoln("Delaying reconnection for ", zkc.reconnDelay)
	<-time.After(zkc.reconnDelay)

	zkc.conn.Close() // kill any current conn.
	return zkc.doConnect()
}

func (zkc *Client) doConnect() error {
	if !zkc.stateChange(connectionRequestedState, connectionAttemptState) {
		log.V(4).Infoln("aborting doConnect, connection attempt already in progress or else disconnected")
		return nil
	}

	// if we're not connected by the time we return then we failed.
	defer func() {
		zkc.stateChange(connectionAttemptState, disconnectedState)
	}()

	// create Connector instance
	conn, sessionEvents, err := zkc.connFactory.create()
	if err != nil {
		return err
	} else {
		log.V(4).Infof("Created connection object of type %T\n", conn)
	}

	zkc.conn = conn
	connected := make(chan struct{})
	go zkc.monitorSession(sessionEvents, connected)

	// wait for connected confirmation
	select {
	case <-connected:
		if !zkc.stateChange(connectionAttemptState, connectedState) {
			log.V(4).Infoln("failed to transition to connected state")
			// we could be:
			// - disconnected        ... reconnect() will try to connect again, otherwise;
			// - connected           ... another goroutine already established a connection
			// - connectionRequested ... another goroutine is already trying to connect
			return zkc.reconnect()
		}
		log.Infoln("zk client connected to server")
		return nil
	case <-time.After(zkc.connTimeout):
		return fmt.Errorf("Unable to confirm connection after %v", zkc.connTimeout)
	}
}

// monitor a zookeeper session event channel, closes the 'connected' channel once
// a zookeeper connection has been established. errors are forwarded to the client's
// errorWatcher. disconnected events are forwarded to client.onDisconnected.
func (zkc *Client) monitorSession(sessionEvents <-chan zk.Event, connected chan struct{}) {
	defer zkc.onDisconnected() // just in case we don't actually get a `disconnected` event from zk lib
	for e := range sessionEvents {
		if e.Err != nil {
			log.Errorf("Received state error: %s", e.Err.Error())
			zkc.errorWatcher.errorOccured(zkc, e.Err)
		}
		switch e.State {
		case zk.StateConnecting:
			log.Infoln("Connecting to zookeeper...")

		case zk.StateConnected:
			close(connected)

		case zk.StateSyncConnected:
			log.Infoln("SyncConnected to zookper server")

		case zk.StateDisconnected:
			zkc.onDisconnected()

		case zk.StateExpired:
			log.Infoln("Zookeeper client session expired, disconnecting.")
		}
	}
}

// watch the child nodes for changes, at the specified path.
// callers that specify a path of `currentPath` will watch the currently set rootPath,
// otherwise the watchedPath is calculated as rootPath+path.
// this func spawns a go routine to actually do the watching, and so returns immediately.
// in the absense of errors a signalling channel is returned that will close
// upon the termination of the watch (e.g. due to disconnection).
func (zkc *Client) watchChildren(path string) (<-chan struct{}, error) {
	if !zkc.isConnected() {
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
	go zkc._watchChildren(watchPath, ch, watchEnded)
	return watchEnded, nil
}

// async continuation of watchChildren
func (zkc *Client) _watchChildren(watchPath string, zkevents <-chan zk.Event, watchEnded chan struct{}) {
	var err error
	for {
		for e := range zkevents {
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
		if !zkc.isConnected() {
			log.V(1).Info("no longer connected to server.")
			close(watchEnded)
			return
		}
		_, _, zkevents, err = zkc.conn.ChildrenW(watchPath)
		if err != nil {
			log.Errorf("unable to watch children for path %s: %s", watchPath, err.Error())
			zkc.errorWatcher.errorOccured(zkc, err)
			return
		}
		//TODO(jdef) any chance of the zkevents chan closing frequently, so as to
		//make this a 'hot' loop? if so, we could implement a backoff here...
		log.V(2).Infoln("rewatching children for path", watchPath)
	}
}

func (zkc *Client) onDisconnected() {
	if st := zkc.getState(); st == connectedState && zkc.stateChange(st, disconnectedState) {
		log.Infoln("disconnected from the server, reconnecting...")
		go func() {
			if err := zkc.reconnect(); err != nil {
				log.Warning(err)
			}
		}()
		return
	}
}

func (zkc *Client) getState() stateType {
	return stateType(atomic.LoadInt32((*int32)(&zkc.state)))
}

// convenience function
func (zkc *Client) isConnected() bool {
	return zkc.getState() == connectedState
}

// convenience function
func (zkc *Client) isConnecting() bool {
	state := zkc.getState()
	return state == connectionRequestedState || state == connectionAttemptState
}

// convenience function
func (zkc *Client) isDisconnected() bool {
	return zkc.getState() == disconnectedState
}

func (zkc *Client) list(path string) ([]string, error) {
	if !zkc.isConnected() {
		return nil, errors.New("Unable to list children, client not connected.")
	}

	children, _, err := zkc.conn.Children(path)
	if err != nil {
		return nil, err
	}

	return children, nil
}

func (zkc *Client) data(path string) ([]byte, error) {
	if !zkc.isConnected() {
		return nil, errors.New("Unable to retrieve node data, client not connected.")
	}

	data, _, err := zkc.conn.Get(path)
	if err != nil {
		return nil, err
	}

	return data, nil
}
