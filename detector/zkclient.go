package detector

import (
	"errors"
	"fmt"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type zkClient struct {
	conn        zkConnector
	hosts       []string
	connTimeout time.Duration
	connected   bool
	stopCh      chan bool
	rootPath    string
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

	conn, ch, err := zk.Connect(zkc.hosts, zkc.connTimeout)
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
					// if c.watcher != nil {
					// 	go c.watcher.Error(e.Err)
					// }
					//zkc.disconnect()
				}
				switch e.State {
				case zk.StateConnecting:
					log.Infoln("Connecting to zookeeper...")

				case zk.StateConnected:
					zkc.connected = true
					log.Infoln("Connected to zookeeper at", zkc.hosts)
					close(waitConnCh)

					// if c.watcher != nil {
					// 	go c.watcher.Connected(c)
					// }

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
	case <-time.After(zkc.connTimeout):
		return fmt.Errorf("Unable to confirm connection after %v.", time.Second*5)
	}

	return nil
}

func (zkc *zkClient) disconnect() error {
	return nil
}

func (zkc *zkClient) watch(path string) error {
	return nil
}
