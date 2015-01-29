package detector

import (
	"errors"
	"fmt"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"sort"
	"time"
)

type zkClient struct {
	conn            zkConnector
	connFactory     zkConnFactory
	hosts           []string
	connTimeout     time.Duration
	connected       bool
	stopCh          chan bool
	rootPath        string
	childrenWatcher zkChildrenWatcher
	errorWatcher    zkErrorWatcher
}

func newZkClient(hosts []string, path string) (*zkClient, error) {
	zkc := new(zkClient)
	zkc.hosts = hosts
	zkc.connTimeout = time.Second * 5
	zkc.rootPath = path

	// TODO: validate  URIs
	return zkc, nil
}

func (zkc *zkClient) connect() error {
	if zkc.connected {
		return nil
	}

	var conn zkConnector
	var ch <-chan zk.Event
	var err error

	if zkc.connFactory == nil {
		var c *zk.Conn
		c, ch, err = zk.Connect(zkc.hosts, zkc.connTimeout)
		conn = zkConnector(c)
	} else {
		conn, ch, err = zkc.connFactory.create()
	}

	if err != nil {
		return err
	}

	zkc.conn = conn

	// make sure connection succeeds: wait for conn notification.
	waitConnCh := make(chan struct{})
	go func() {
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
					zkc.connected = true
					log.Infoln("ZkClient connected to server.")
					close(waitConnCh)

				case zk.StateSyncConnected:
					zkc.connected = true
					log.Infoln("SyncConnected to zookper server")
				case zk.StateDisconnected:
					log.Infoln("Disconnected from zookeeper server")
					zkc.disconnect()
				case zk.StateExpired:
					log.Infoln("Zookeeper client session expired, disconnecting.")
					//zkc.disconnect()
				}
			}
		}
	}()

	// wait for connected confirmation
	select {
	case <-waitConnCh:
		if !zkc.connected {
			err := errors.New("Unabe to confirm connected state.")
			log.Errorf(err.Error())
			return err
		}
		log.V(2).Infoln("Connection confirmed.")
	case <-time.After(zkc.connTimeout):
		return fmt.Errorf("Unable to confirm connection after %v.", time.Second*5)
	}

	return nil
}

func (zkc *zkClient) disconnect() error {
	return nil
}

func (zkc *zkClient) watchChildren(path string) error {
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

func (zkc *zkClient) list(path string) ([]string, error) {
	if !zkc.connected {
		return nil, errors.New("Unable to list children, client not connected.")
	}

	children, _, err := zkc.conn.Children(path)
	if err != nil {
		return nil, err
	}

	// sort children (ascending).
	sort.Strings(children)
	return children, nil
}

func (zkc *zkClient) data(path string) ([]byte, error) {
	if !zkc.connected {
		return nil, errors.New("Unable to retrieve node data, client not connected.")
	}

	data, _, err := zkc.conn.Get(path)
	if err != nil {
		return nil, err
	}

	return data, nil
}
