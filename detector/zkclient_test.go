package detector

import (
	"strings"
	// log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"testing"
	// "time"
	"os"
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

// This test requires zookeeper to be running and env ZK_HOSTS set.
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

func makeMockConnector(path string, chEvent <-chan zk.Event) *MockZkConnector {
	conn := NewMockZkConnector()
	conn.On("Close").Return(nil)
	conn.On("ChildrenW", path).Return([]string{path}, &zk.Stat{}, chEvent, nil)
	conn.On("Children").Return([]string{"x", "a", "d"}, &zk.Stat{}, nil)
	conn.On("Get", path).Return([]byte("Hello"), &zk.Stat{}, nil)

	return conn
}
