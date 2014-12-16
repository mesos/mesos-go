/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zookeeper

import (
	"errors"
	"fmt"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

type ZkClient struct {
	Conn      ZkConn
	connected bool
	stopCh    chan bool

	watcher  ZkClientWatcher
	rootNode ZkNode
	hosts    []string
}

func NewZkClient(watcher ZkClientWatcher) *ZkClient {
	return &ZkClient{watcher: watcher}
}

func (c *ZkClient) Connect(uris []string, path string) error {
	if c.connected {
		return nil
	}

	c.rootNode = NewZkDataNode(c, path)
	conn, ch, err := zk.Connect(uris, time.Second*5)
	if err != nil {
		return err
	}

	c.Conn = conn

	waitConnCh := make(chan struct{})
	go func() {
		for {
			select {
			case e := <-ch:
				if e.Err != nil {
					log.Errorf("Received state error: %s", e.Err.Error())
					if c.watcher != nil {
						go c.watcher.Error(e.Err)
					}
					c.Disconnect()
				}
				switch e.State {
				case zk.StateConnecting:
					log.Infoln("Connecting to zookeeper...")

				case zk.StateConnected:
					c.connected = true
					log.Infoln("Connected to zookeeper at", uris)
					close(waitConnCh)
					if c.watcher != nil {
						go c.watcher.Connected(c)
					}

				case zk.StateSyncConnected:
					c.connected = true
					log.Infoln("SyncConnected to zookper server")
				case zk.StateDisconnected:
					log.Infoln("Disconnected from zookeeper server")
					c.Disconnect()
				case zk.StateExpired:
					log.Infoln("Zookeeper client session expired, disconnecting.")
					c.Disconnect()
				}
			}
		}
	}()

	// wait for connected confirmation
	select {
	case <-waitConnCh:
		if !c.connected {
			err := errors.New("Unabe to confirm connected state.")
			log.Errorf(err.Error())
			return err
		}
	case <-time.After(time.Second * 5):
		return fmt.Errorf("Unable to confirm connection after %v.", time.Second*5)
	}

	return nil
}

func (c *ZkClient) WatchChildren(path string) error {
	if !c.connected {
		return errors.New("Not connected to server.")
	}
	watchPath := c.rootNode.String()
	if path != "" && path != "." {
		watchPath = watchPath + path
	}

	log.V(2).Infoln("Watching children for path", watchPath)
	children, _, ch, err := c.Conn.ChildrenW(watchPath)
	if err != nil {
		return err
	}

	go func(chList []string) {
		select {
		case e := <-ch:
			if e.Err != nil {
				log.Errorf("Received error while watching path %s:%s", watchPath, e.Err.Error())
				if c.watcher != nil {
					c.watcher.Error(e.Err)
				}
			}

			switch e.Type {
			case zk.EventNodeChildrenChanged:
				node := NewZkDataNode(c, e.Path)
				if c.watcher != nil {
					c.watcher.ChildrenChanged(c, node)
				}
			}
		}
		err := c.WatchChildren(path)
		if err != nil {
			log.Errorf("Unable to watch children for path %s: %s", path, err.Error())
			//c.stopCh <- true
		}
	}(children)
	return nil
}

func (c *ZkClient) Disconnect() {
	c.Conn.Close()
	c.connected = false
	//c.stopCh <- true
}
