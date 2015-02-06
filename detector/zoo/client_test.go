package zoo

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

var test_zk_hosts = []string{"localhost:2181"}
var test_zk_path = "/test"

func TestClientNew(t *testing.T) {
	path := "/mesos"
	chEvent := make(chan zk.Event)
	connector := makeMockConnector(path, chEvent)

	c, err := newClient(test_zk_hosts, path)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.False(t, c.connected)
	c.conn = connector

}

// This test requires zookeeper to be running.
// You must also set env variable ZK_HOSTS to point to zk hosts.
// The zk package does not offer a way to mock its connection function.
func TestClientConnectIntegration(t *testing.T) {
	if os.Getenv("ZK_HOSTS") == "" {
		t.Skip("Skipping zk-server connection test: missing env ZK_HOSTS.")
	}
	hosts := strings.Split(os.Getenv("ZK_HOSTS"), ",")
	c, err := newClient(hosts, "/mesos")
	assert.NoError(t, err)
	err = c.connect()
	assert.NoError(t, err)

	err = c.connect()
	assert.NoError(t, err)
	assert.True(t, c.connected)
}

func TestClientConnect(t *testing.T) {
	c, err := makeClient()
	assert.NoError(t, err)
	assert.False(t, c.connected)
	c.connect()
	assert.True(t, c.connected)
	assert.False(t, c.connecting)
}

func TestClientDisconnect(t *testing.T) {
	c, err := makeClient()
	assert.False(t, c.connected)
	err = c.connect()
	assert.NoError(t, err)
	assert.True(t, c.connected)
	err = c.disconnect()
	assert.NoError(t, err)
	assert.False(t, c.connected)
}

func TestClientDisconnectedEvent(t *testing.T) {
	ch0 := make(chan zk.Event, 3)
	ch1 := make(chan zk.Event, 1)

	c, err := newClient(test_zk_hosts, test_zk_path)
	assert.NoError(t, err)

	c.connFactory = asFactory(func() (Connector, <-chan zk.Event, error) {
		log.V(2).Infof("**** Using zk.Conn adapter ****")
		connector := makeMockConnector(test_zk_path, ch1)
		return connector, ch0, nil
	})

	// put connecting, connected events.
	ch0 <- zk.Event{
		State: zk.StateConnecting,
		Path:  test_zk_path,
	}
	ch0 <- zk.Event{
		State: zk.StateConnected,
		Path:  test_zk_path,
	}
	time.Sleep(time.Millisecond * 7)
	assert.False(t, c.connected)
	c.connect()
	assert.True(t, c.connected)
	assert.False(t, c.connecting)

	//TODO(vlv) - Need to add discontinuity test.

	// send disconnecting
	// c.reconnDelay = time.Millisecond * 100

	// ch0 <- zk.Event{
	// 	State: zk.StateDisconnected,
	// 	Path:  test_zk_path,
	// }
	// time.Sleep(time.Millisecond * 5000)
	// assert.True(t, c.connected)
	// assert.False(t, c.connecting)

}

func TestClientWatchChildren(t *testing.T) {
	c, err := makeClient()
	assert.NoError(t, err)
	err = c.connect()
	assert.NoError(t, err)
	wCh := make(chan struct{}, 1)
	c.childrenWatcher = asChildWatcher(func(zkc *Client, path string) {
		log.V(4).Infoln("Path", path, "changed!")
		children, err := c.list(path)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(children))
		assert.Equal(t, "info_0", children[0])
		assert.Equal(t, "info_5", children[1])
		assert.Equal(t, "info_10", children[2])
		wCh <- struct{}{}
	})

	err = c.watchChildren(".")
	assert.NoError(t, err)

	select {
	case <-wCh:
	case <-time.After(time.Millisecond * 700):
		panic("Waited too long...")
	}
}

func TestClientWatchErrors(t *testing.T) {
	path := "/test"
	ch := make(chan zk.Event, 1)
	ch <- zk.Event{
		Type: zk.EventNodeChildrenChanged,
		Path: "/test",
		Err:  errors.New("Event Error"),
	}

	c, err := makeClient()
	c.connected = true
	assert.NoError(t, err)
	c.conn = makeMockConnector(path, (<-chan zk.Event)(ch))
	wCh := make(chan struct{}, 1)
	c.errorWatcher = asErrorWatcher(func(zkc *Client, err error) {
		assert.Error(t, err)
		wCh <- struct{}{}
	})

	c.watchChildren(".")

	select {
	case <-wCh:
	case <-time.After(time.Millisecond * 700):
		panic("Waited too long...")
	}

}

func makeClient() (*Client, error) {
	ch0 := make(chan zk.Event, 1)
	ch1 := make(chan zk.Event, 1)

	ch0 <- zk.Event{
		State: zk.StateConnected,
		Path:  test_zk_path,
	}

	ch1 <- zk.Event{
		Type: zk.EventNodeChildrenChanged,
		Path: test_zk_path,
	}

	c, err := newClient(test_zk_hosts, test_zk_path)
	if err != nil {
		return nil, err
	}

	c.connFactory = asFactory(func() (Connector, <-chan zk.Event, error) {
		log.V(2).Infof("**** Using zk.Conn adapter ****")
		connector := makeMockConnector(test_zk_path, ch1)
		return connector, ch0, nil
	})

	return c, nil
}

func makeMockConnector(path string, chEvent <-chan zk.Event) *MockConnector {
	log.V(2).Infoln("Making Connector mock.")
	conn := NewMockConnector()
	conn.On("Close").Return(nil)
	conn.On("ChildrenW", path).Return([]string{path}, &zk.Stat{}, chEvent, nil)
	conn.On("Children", path).Return([]string{"info_0", "info_5", "info_10"}, &zk.Stat{}, nil)
	conn.On("Get", path).Return(makeTestMasterInfo(), &zk.Stat{}, nil)

	return conn
}

func makeTestMasterInfo() []byte {
	miPb := util.NewMasterInfo("master@localhost:5050", 123456789, 400)
	data, err := proto.Marshal(miPb)
	if err != nil {
		panic(err)
	}
	return data
}
