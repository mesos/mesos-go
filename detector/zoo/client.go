package zoo

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	defaultSessionTimeout   = 60 * time.Second
	defaultReconnectTimeout = 5 * time.Second
	currentPath             = "."
)

type stateType int32

const (
	disconnectedState stateType = iota
	connectionRequestedState
	connectionAttemptState
	connectedState
)

type Client struct {
	conn           Connector
	defaultFactory Factory
	factory        Factory // must never be nil, use setFactory to update
	state          stateType
	reconnCount    uint64
	reconnDelay    time.Duration
	rootPath       string
	errorHandler   ErrorHandler // must never be nil
	connectOnce    sync.Once
	stopOnce       sync.Once
	shouldStop     chan struct{} // signal chan
	shouldReconn   chan struct{} // message chan
	connLock       sync.Mutex
}

func newClient(hosts []string, path string) (*Client, error) {
	zkc := &Client{
		reconnDelay:  defaultReconnectTimeout,
		rootPath:     path,
		shouldStop:   make(chan struct{}),
		shouldReconn: make(chan struct{}, 1),
		errorHandler: ErrorHandler(func(*Client, error) {}),
		defaultFactory: asFactory(func() (Connector, <-chan zk.Event, error) {
			return zk.Connect(hosts, defaultSessionTimeout)
		}),
	}
	zkc.setFactory(zkc.defaultFactory)
	// TODO(vlad): validate  URIs
	return zkc, nil
}

func (zkc *Client) setFactory(f Factory) {
	if f == nil {
		f = zkc.defaultFactory
	}
	zkc.factory = asFactory(func() (c Connector, ch <-chan zk.Event, err error) {
		select {
		case <-zkc.shouldStop:
			err = errors.New("client stopping")
		default:
			zkc.connLock.Lock()
			defer zkc.connLock.Unlock()
			if zkc.conn != nil {
				zkc.conn.Close()
			}
			c, ch, err = f.create()
			zkc.conn = c
		}
		return
	})
}

// return true only if the client's state was changed from `from` to `to`
func (zkc *Client) stateChange(from, to stateType) bool {
	return atomic.CompareAndSwapInt32((*int32)(&zkc.state), int32(from), int32(to))
}

// connect to zookeeper, blocks on the initial call to doConnect()
func (zkc *Client) connect() {
	select {
	case <-zkc.shouldStop:
		return
	default:
		zkc.connectOnce.Do(func() {
			if zkc.stateChange(disconnectedState, connectionRequestedState) {
				if err := zkc.doConnect(); err != nil {
					zkc.errorHandler(zkc, err)
				}
			}
			go func() {
				for {
					select {
					case <-zkc.shouldStop:
						zkc.connLock.Lock()
						defer zkc.connLock.Unlock()
						if zkc.conn != nil {
							zkc.conn.Close()
						}
						return
					case <-zkc.shouldReconn:
						if err := zkc.reconnect(); err != nil {
							zkc.errorHandler(zkc, err)
						}
					}
				}
			}()
		})
	}
	return
}

// attempt to reconnect to zookeeper. will ignore attempts to reconnect
// if not disconnected. if reconnection is attempted then this func will block
// for at least reconnDelay before actually attempting to connect to zookeeper.
func (zkc *Client) reconnect() error {
	if !zkc.stateChange(disconnectedState, connectionRequestedState) {
		log.V(4).Infoln("Ignoring reconnect, currently connected/connecting.")
		return nil
	}

	defer func() { zkc.reconnCount++ }()

	log.V(4).Infoln("Delaying reconnection for ", zkc.reconnDelay)
	<-time.After(zkc.reconnDelay)

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
	conn, sessionEvents, err := zkc.factory.create()
	if err != nil {
		// once the factory stops producing connectors, it's time to stop
		zkc.stop()
		return err
	}
	log.V(4).Infof("Created connection object of type %T\n", conn)
	connected := make(chan struct{})
	sessionExpired := make(chan struct{})
	go func() {
		defer close(sessionExpired)
		zkc.monitorSession(sessionEvents, connected)
	}()

	// wait for connected confirmation
	select {
	case <-connected:
		if !zkc.stateChange(connectionAttemptState, connectedState) {
			log.V(4).Infoln("failed to transition to connected state")
			// we could be:
			// - disconnected        ... reconnect() will try to connect again, otherwise;
			// - connected           ... another goroutine already established a connection
			// - connectionRequested ... another goroutine is already trying to connect
			zkc.requestReconnect()
		}
		log.Infoln("zookeeper client connected")
	case <-sessionExpired:
		// connection was disconnected before it was ever really 'connected'
		if !zkc.stateChange(connectionAttemptState, disconnectedState) {
			//programming error
			panic("failed to transition from connection-attempt to disconnected state")
		}
		zkc.requestReconnect()
	case <-zkc.shouldStop:
		// noop
	}
	return nil
}

// signal for reconnect unless we're shutting down
func (zkc *Client) requestReconnect() {
	select {
	case <-zkc.shouldStop:
		// abort reconnect request, client is shutting down
	default:
		select {
		case zkc.shouldReconn <- struct{}{}:
			// reconnect request successful
		default:
			// reconnect chan is full: reconnect has already
			// been requested. move on.
		}
	}
}

// monitor a zookeeper session event channel, closes the 'connected' channel once
// a zookeeper connection has been established. errors are forwarded to the client's
// errorHandler. disconnected events are forwarded to client.onDisconnected.
// this func blocks until either the client's shouldStop or sessionEvents chan are closed.
func (zkc *Client) monitorSession(sessionEvents <-chan zk.Event, connected chan struct{}) {
	for {
		select {
		case <-zkc.shouldStop:
			return
		case e, ok := <-sessionEvents:
			if !ok {
				return
			} else if e.Err != nil {
				log.Errorf("received state error: %s", e.Err.Error())
				zkc.errorHandler(zkc, e.Err)
			}
			switch e.State {
			case zk.StateConnecting:
				log.Infoln("connecting to zookeeper..")

			case zk.StateConnected:
				close(connected)

			case zk.StateSyncConnected:
				log.Infoln("syncConnected to zookper server")

			case zk.StateDisconnected:
				log.Infoln("zookeeper client disconnected")
				zkc.onDisconnected()

			case zk.StateExpired:
				log.Infoln("zookeeper client session expired")
			}
		}
	}
}

// watch the child nodes for changes, at the specified path.
// callers that specify a path of `currentPath` will watch the currently set rootPath,
// otherwise the watchedPath is calculated as rootPath+path.
// this func spawns a go routine to actually do the watching, and so returns immediately.
// in the absense of errors a signalling channel is returned that will close
// upon the termination of the watch (e.g. due to disconnection).
func (zkc *Client) watchChildren(path string, watcher ChildWatcher) (<-chan struct{}, error) {
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
	go func() {
		defer close(watchEnded)
		zkc._watchChildren(watchPath, ch, watcher)
	}()
	return watchEnded, nil
}

// async continuation of watchChildren
func (zkc *Client) _watchChildren(watchPath string, zkevents <-chan zk.Event, watcher ChildWatcher) {
	var err error
	for {
	eventLoop:
		for {
			select {
			case <-zkc.shouldStop:
				return
			case e, ok := <-zkevents:
				if !ok {
					break eventLoop
				} else if e.Err != nil {
					log.Errorf("Received error while watching path %s: %s", watchPath, e.Err.Error())
					zkc.errorHandler(zkc, e.Err)
				}
				switch e.Type {
				case zk.EventNodeChildrenChanged:
					log.V(2).Infoln("Handling: zk.EventNodeChildrenChanged")
					watcher(zkc, e.Path)
				}
			}
		}
		if !zkc.isConnected() {
			log.V(1).Info("no longer connected to server.")
			return
		}
		_, _, zkevents, err = zkc.conn.ChildrenW(watchPath)
		if err != nil {
			log.Errorf("unable to watch children for path %s: %s", watchPath, err.Error())
			zkc.errorHandler(zkc, err)
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
		zkc.requestReconnect()
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

func (zkc *Client) stop() {
	zkc.stopOnce.Do(func() {
		close(zkc.shouldStop)
	})
}

// when this channel is closed the client is either stopping, or has stopped
func (zkc *Client) stopped() <-chan struct{} {
	return zkc.shouldStop
}
