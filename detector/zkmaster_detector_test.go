package detector

import (
	_ "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	_ "github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
	"time"
)

var zkurl = "zk://127.0.0.1:2181/mesos"
var zkurl_bad = "zk://127.0.0.1:2181"

func TestZkMasterDetectorNew(t *testing.T) {
	md, err := NewZkMasterDetector(zkurl)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(md.zkHosts))
	u, _ := url.Parse(zkurl)
	assert.True(t, u.String() == md.url.String())
	assert.Equal(t, "/mesos", md.zkPath)
}

func TestZkMasterDetectorStart(t *testing.T) {
	c, err := makeZkClient()
	assert.False(t, c.connected)
	md, err := NewZkMasterDetector(zkurl)
	assert.NoError(t, err)
	md.zkClient = c // override zk.Conn with our own.
	err = md.Start()
	assert.NoError(t, err)
	assert.True(t, c.connected)
}

func TestZkMasterDetectorChildrenChanged(t *testing.T) {
	wCh := make(chan struct{}, 1)

	c, err := makeZkClient()
	assert.NoError(t, err)
	assert.False(t, c.connected)

	md, err := NewZkMasterDetector(zkurl)
	assert.NoError(t, err)
	// override zk.Conn with our own.
	md.zkClient = c
	md.zkClient.childrenWatcher = zkChildrenWatcherFunc(md.childrenChanged)

	err = md.Start()
	assert.NoError(t, err)
	assert.True(t, c.connected)

	md.Detect(func(master *mesos.MasterInfo) {
		println("Hello!")
		wCh <- struct{}{}
	})

	select {
	case <-wCh:
	case <-time.After(time.Second * 3):
		panic("Waited too long...")
	}

}

// type testDataNode string

// func (n testDataNode) Data() ([]byte, error) {
// 	return []byte("Hello"), nil
// }

// func (n testDataNode) List() ([]zookeeper.ZkNode, error) {
// 	return []zookeeper.ZkNode{
// 		testDataNode("info_001"),
// 		testDataNode("info_002"),
// 		testDataNode("info_003"),
// 	}, nil
// }

// func (n testDataNode) String() string {
// 	return string(n)
// }

// func createClient(watcher zookeeper.ZkWatcher) *zookeeper.ZkClientConnector {
// 	c := zookeeper.NewZkClientConnector()
// 	conn := NewMockZkConnector()
// 	conn.On("Close").Return(nil)
// 	conn.On("ChildrenW", path).Return([]string{path}, &zk.Stat{}, chEvent, nil)
// 	conn.On("Children").Return([]string{"x", "a", "d"}, &zk.Stat{}, nil)
// 	conn.On("Get", path).Return([]byte("Hello"), &zk.Stat{}, nil)
// 	c.Conn = conn
// 	c.connected = true
// 	c.rootNode = NewZkPath(c, path)
// 	return c

// 	// c := zookeeper.New()
// 	// c.Connected = true
// 	// c.Watcher = watcher
// 	// c.WatchChildren("/mesos")
// 	// c.On("Connect").Return(nil)
// 	// c.On("WatchChildren").Return(nil)
// 	// c.On("Disconnect").Return()

// 	// return c
// }
