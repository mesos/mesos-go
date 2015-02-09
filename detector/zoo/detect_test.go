package zoo

import (
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/detector"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

var zkurl = "zk://127.0.0.1:2181/mesos"
var zkurl_bad = "zk://127.0.0.1:2181"

func TestMasterDetectorNew(t *testing.T) {
	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(md.zkHosts))
	u, _ := url.Parse(zkurl)
	assert.True(t, u.String() == md.url.String())
	assert.Equal(t, "/mesos", md.zkPath)
}

func TestMasterDetectorStart(t *testing.T) {
	c, err := makeClient()
	assert.False(t, c.connected)
	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)
	md.client = c // override zk.Conn with our own.
	err = md.Start()
	assert.NoError(t, err)
	assert.True(t, c.connected)
}

func TestMasterDetectorChildrenChanged(t *testing.T) {
	wCh := make(chan struct{}, 1)

	c, err := makeClient()
	assert.NoError(t, err)
	assert.False(t, c.connected)

	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)
	// override zk.Conn with our own.
	md.client = c
	md.client.childrenWatcher = asChildWatcher(md.childrenChanged)

	err = md.Start()
	assert.NoError(t, err)
	assert.True(t, c.connected)

	md.Detect(detector.AsMasterChanged(func(master *mesos.MasterInfo) {
		assert.NotNil(t, master)
		assert.Equal(t, master.GetId(), "master@localhost:5050")
		wCh <- struct{}{}
	}))

	select {
	case <-wCh:
	case <-time.After(time.Second * 3):
		panic("Waited too long...")
	}
}

func TestMasterDetectMultiple(t *testing.T) {
	ch0 := make(chan zk.Event, 1)
	ch1 := make(chan zk.Event, 1)
	var wg sync.WaitGroup

	ch0 <- zk.Event{
		State: zk.StateConnected,
		Path:  test_zk_path,
	}

	c, err := newClient(test_zk_hosts, test_zk_path)
	assert.NoError(t, err)

	connector := makeMockConnector(test_zk_path, ch1)
	c.connFactory = asFactory(func() (Connector, <-chan zk.Event, error) {
		log.V(2).Infof("**** Using zk.Conn adapter ****")
		return connector, ch0, nil
	})

	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)
	md.client = c
	md.client.childrenWatcher = asChildWatcher(md.childrenChanged)

	err = md.Start()
	assert.NoError(t, err)
	assert.True(t, c.connected)

	md.Detect(detector.AsMasterChanged(func(master *mesos.MasterInfo) {
		log.V(2).Infoln("Leader change detected.")
		wg.Done()
	}))

	// **** Test 4 consecutive ChildrenChangedEvents ******
	// setup event changes
	sequences := [][]string{
		[]string{"info_005", "info_010", "info_022"},
		[]string{"info_014", "info_010", "info_005"},
		[]string{"info_005", "info_004", "info_022"},
		[]string{"info_017", "info_099", "info_200"},
	}
	wg.Add(3) // leader changes 3 times.

	go func() {
		conn := NewMockConnector()
		md.client.conn = conn

		for i := range sequences {
			path := "/test" + strconv.Itoa(i)
			conn.On("ChildrenW", path).Return([]string{path}, &zk.Stat{}, (<-chan zk.Event)(ch1), nil)
			conn.On("Children", path).Return(sequences[i], &zk.Stat{}, nil)
			conn.On("Get", path).Return(makeTestMasterInfo(), &zk.Stat{}, nil)
			md.client.rootPath = path
			ch1 <- zk.Event{
				Type: zk.EventNodeChildrenChanged,
				Path: path,
			}
		}
	}()

	wg.Wait()
}

func TestMasterDetect_none(t *testing.T) {
	assert := assert.New(t)
	nodeList := []string{}
	node := selectTopNode(nodeList)
	assert.Equal("", node)
}
