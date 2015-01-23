package detector

import (
	"errors"
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
	"time"
)

var test_zk_hosts = []string{"localhost:2181"}

func TestZkClientNew(t *testing.T) {
	path := "/mesos"
	chEvent := make(chan zk.Event)
	connector := makeMockConnector(path, chEvent)

	c, err := newZkClient(test_zk_hosts, path)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.False(t, c.connected)
	c.conn = connector

}

// This test requires zookeeper to be running.
// You must also set env variable ZK_HOSTS to point to zk hosts.
// The zk package does not offer a way to mock its connection function.
func TestZkClientConnect(t *testing.T) {
	if os.Getenv("ZK_HOSTS") == "" {
		t.Skip("Skipping test: requires env ZK_HOSTS for zookeeper addresses.")
	}
	hosts := strings.Split(os.Getenv("ZK_HOSTS"), ",")
	c, err := newZkClient(hosts, "/mesos")
	assert.NoError(t, err)
	err = c.connect()
	assert.NoError(t, err)

	err = c.connect()
	assert.NoError(t, err)
	assert.True(t, c.connected)
}

func TestWatchChildren(t *testing.T) {
	path := "/test"
	ch := make(chan zk.Event, 1)
	e := zk.Event{
		Type: zk.EventNodeChildrenChanged,
		Path: path,
	}

	c := makeZkClient(t, test_zk_hosts, path)
	c.conn = makeMockConnector(path, (<-chan zk.Event)(ch))
	wCh := make(chan struct{}, 1)
	c.childrenWatcher = zkChildrenWatcherFunc(func(zkc *zkClient, path string) {
		log.V(4).Infoln("Path", path, "changed!")
		children, err := c.list(path)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(children))
		// assert sorted children
		assert.Equal(t, "a", children[0])
		assert.Equal(t, "d", children[1])
		assert.Equal(t, "x", children[2])
		wCh <- struct{}{}
	})

	err := c.watchChildren(".")
	assert.NoError(t, err)

	ch <- e // signal nodechildrenchanged event.

	select {
	case <-wCh:
	case <-time.After(time.Millisecond * 700):
		panic("Waited too long...")
	}
}

func TestWatchErrors(t *testing.T) {
	path := "/test"
	ch := make(chan zk.Event, 1)
	e := zk.Event{
		Type: zk.EventNodeChildrenChanged,
		Path: "/test",
		Err:  errors.New("Event Error"),
	}

	c := makeZkClient(t, test_zk_hosts, path)
	c.conn = makeMockConnector(path, (<-chan zk.Event)(ch))
	wCh := make(chan struct{}, 1)
	c.errorWatcher = zkErrorWatcherFunc(func(zkc *zkClient, err error) {
		wCh <- struct{}{}
	})

	c.watchChildren(".")
	ch <- e // signal nodechildrenchanged event.

	select {
	case <-wCh:
	case <-time.After(time.Millisecond * 700):
		panic("Waited too long...")
	}

}

func makeZkClient(t *testing.T, hosts []string, path string) *zkClient {
	c, err := newZkClient(hosts, path)
	assert.NoError(t, err)
	c.connected = true
	return c
}

func makeMockConnector(path string, chEvent <-chan zk.Event) *MockZkConnector {
	conn := NewMockZkConnector()
	conn.On("Close").Return(nil)
	conn.On("ChildrenW", path).Return([]string{path}, &zk.Stat{}, chEvent, nil)
	conn.On("Children").Return([]string{"x", "a", "d"}, &zk.Stat{}, nil)
	conn.On("Get", path).Return([]byte("Hello"), &zk.Stat{}, nil)

	return conn
}
