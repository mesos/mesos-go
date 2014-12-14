// +test zk-test
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

// Testing code for the zookeeper client.
// All integrative tests will require a running zookeeper instance.
// Set OS ENV variables: $ZK_SERVER="<address>:<port>"
// If variable not set, integrative tests will be skipped.
package zookeeper

import (
	"errors"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testClientWatcher struct {
	t  *testing.T
	ch chan bool
}

func newTestClientWatcher() *testClientWatcher {
	return &testClientWatcher{ch: make(chan bool)}
}

func (w *testClientWatcher) Connected(c *ZkClient) {
	log.V(2).Infoln("Connected")
	w.ch <- true
}

func (w *testClientWatcher) ChildrenChanged(c *ZkClient, node ZkNode) {
	log.V(2).Infoln("Children changed")
	list, err := node.List()
	assert.NoError(w.t, err)
	assert.Equal(w.t, 3, len(list))
	w.ch <- true
}

func (w *testClientWatcher) Error(err error) {
	log.Errorf(err.Error())
	w.ch <- true
}

// ----------------------- Test Functions ---------------------- //
func TestZkClientCreate(t *testing.T) {
	c := NewZkClient(&testClientWatcher{})
	assert.NotNil(t, c)
	assert.False(t, c.connected)
}

func TestZkClientWatchChildren(t *testing.T) {
	ch := make(chan zk.Event, 1)
	e := zk.Event{
		Type: zk.EventNodeChildrenChanged,
		Path: "/test",
	}
	ch <- e

	w := newTestClientWatcher()
	w.t = t
	c := createClient("/test", (<-chan zk.Event)(ch))
	c.Watcher = w
	c.WatchChildren(".")

	select {
	case <-w.ch:
	case <-time.After(time.Millisecond * 700):
		panic("Waited too long for connection.")
	}
}

func TestZkClientWatchChildren_WithError(t *testing.T) {
	c := createClientWithError("/test", make(<-chan zk.Event))
	err := c.WatchChildren(".")
	assert.Error(t, err)
}

func TestZkClientWatchChildren_WithEventError(t *testing.T) {
	ch := make(chan zk.Event, 1)
	e := zk.Event{
		Type: zk.EventNodeChildrenChanged,
		Path: "/test",
		Err:  errors.New("Event Error"),
	}
	ch <- e

	w := newTestClientWatcher()
	w.t = t
	c := createClient("/test", (<-chan zk.Event)(ch))
	c.Watcher = w
	c.WatchChildren(".")

	select {
	case <-w.ch:
	case <-time.After(time.Millisecond * 700):
		panic("Waited too long for connection.")
	}
}

func TestZkNodeData(t *testing.T) {
	c := createClient("/test", make(<-chan zk.Event))
	node := NewZkDataNode(c, "/test")
	data, err := node.Data()
	assert.NoError(t, err)
	assert.Equal(t, []byte("Hello"), data)
}

func TestZkNodeDataWithError(t *testing.T) {
	c := createClientWithError("/test", make(<-chan zk.Event))
	node := NewZkDataNode(c, "/test")
	data, err := node.Data()
	assert.Error(t, err)
	assert.Nil(t, data)
}

func TestZkNodeList(t *testing.T) {
	c := createClient("/test", make(<-chan zk.Event))
	node := NewZkDataNode(c, "/test")
	nodes, err := node.List()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(nodes))
}

func TestZkNodeListWithError(t *testing.T) {
	c := createClientWithError("/test", make(<-chan zk.Event))
	node := NewZkDataNode(c, "/test")
	nodes, err := node.List()
	assert.Error(t, err)
	assert.Nil(t, nodes)
}

func createClient(path string, chEvent <-chan zk.Event) *ZkClient {
	c := NewZkClient(newTestClientWatcher())
	conn := NewMockZkConn()
	conn.On("Close").Return(nil)
	conn.On("ChildrenW", path).Return([]string{path}, &zk.Stat{}, chEvent, nil)
	conn.On("Children").Return([]string{"a", "b", "c"}, &zk.Stat{}, nil)
	conn.On("Get", path).Return([]byte("Hello"), &zk.Stat{}, nil)
	c.conn = conn
	c.connected = true
	c.RootNode = NewZkDataNode(c, path)
	return c
}

func createClientWithError(path string, chEvent <-chan zk.Event) *ZkClient {
	c := NewZkClient(newTestClientWatcher())
	conn := NewMockZkConn()
	conn.On("Close").Return(nil)
	conn.On("ChildrenW", path).Return([]string{path}, &zk.Stat{}, chEvent, errors.New("ChildrenW() eror"))
	conn.On("Children").Return([]string{"a", "b", "c"}, &zk.Stat{}, errors.New("Children() error"))
	conn.On("Get", path).Return([]byte("Hello"), &zk.Stat{}, errors.New("Get() error"))
	c.conn = conn
	c.connected = true
	c.RootNode = NewZkDataNode(c, path)
	return c
}
