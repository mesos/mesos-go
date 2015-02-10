package zoo

import (
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/detector"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

const (
	zkurl     = "zk://127.0.0.1:2181/mesos"
	zkurl_bad = "zk://127.0.0.1:2181"
)

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
	assert.False(t, c.isConnected())
	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)
	c.errorHandler = ErrorHandler(func(c *Client, e error) {
		err = e
	})
	md.client = c // override zk.Conn with our own.
	md.Start()
	assert.NoError(t, err)
	assert.True(t, c.isConnected())
}

func TestMasterDetectorChildrenChanged(t *testing.T) {
	wCh := make(chan struct{}, 1)

	c, err := makeClient()
	assert.NoError(t, err)
	assert.False(t, c.isConnected())

	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)
	// override zk.Conn with our own.
	c.errorHandler = ErrorHandler(func(c *Client, e error) {
		err = e
	})
	md.client = c

	md.Start()
	assert.NoError(t, err)
	assert.True(t, c.isConnected())

	called := 0
	md.Detect(detector.AsMasterChanged(func(master *mesos.MasterInfo) {
		//expect 2 calls in sequence: the first setting a master
		//and the second clearing it
		switch called++; called {
		case 1:
			assert.NotNil(t, master)
			assert.Equal(t, master.GetId(), "master@localhost:5050")
			wCh <- struct{}{}
		case 2:
			assert.Nil(t, master)
			wCh <- struct{}{}
		default:
			t.Fatalf("unexpected notification call attempt %d", called)
		}
	}))

	startWait := time.Now()
	select {
	case <-wCh:
	case <-time.After(time.Second * 3):
		panic("Waited too long...")
	}

	// wait for the disconnect event, should be triggered
	// 1s after the connected event
	waited := time.Now().Sub(startWait)
	time.Sleep((2 * time.Second) - waited)
	assert.False(t, c.isConnected())
}

func TestMasterDetectMultiple(t *testing.T) {
	ch0 := make(chan zk.Event, 5)
	ch1 := make(chan zk.Event, 5)

	ch0 <- zk.Event{
		State: zk.StateConnected,
		Path:  test_zk_path,
	}

	c, err := newClient(test_zk_hosts, test_zk_path)
	assert.NoError(t, err)

	connector := NewMockConnector()
	connector.On("Close").Return(nil)
	connector.On("ChildrenW", test_zk_path).Return([]string{test_zk_path}, &zk.Stat{}, (<-chan zk.Event)(ch1), nil)

	first := true
	c.setFactory(asFactory(func() (Connector, <-chan zk.Event, error) {
		log.V(2).Infof("**** Using zk.Conn adapter ****")
		if !first {
			return nil, nil, errors.New("only 1 connector allowed")
		} else {
			first = false
		}
		return connector, ch0, nil
	}))

	md, err := NewMasterDetector(zkurl)
	assert.NoError(t, err)

	c.errorHandler = ErrorHandler(func(c *Client, e error) {
		err = e
	})
	md.client = c
	md.Start()
	assert.NoError(t, err)
	assert.True(t, c.isConnected())

	// **** Test 4 consecutive ChildrenChangedEvents ******
	// setup event changes
	sequences := [][]string{
		[]string{"info_005", "info_010", "info_022"},
		[]string{"info_014", "info_010", "info_005"},
		[]string{"info_005", "info_004", "info_022"},
		[]string{"info_017", "info_099", "info_200"},
	}

	var wg sync.WaitGroup
	startTime := time.Now()
	md.Detect(detector.AsMasterChanged(func(master *mesos.MasterInfo) {
		t.Logf("Leader change detected at %v: %+v", time.Now().Sub(startTime), master)
		wg.Done()
	}))

	// 3 leadership changes + disconnect (leader change to '')
	wg.Add(4)

	go func() {
		for i := range sequences {
			t.Logf("testing master change sequence %d", i)
			connector.On("Children", test_zk_path).Return(sequences[i], &zk.Stat{}, nil).Once()
			connector.On("Get", test_zk_path).Return(newTestMasterInfo(i), &zk.Stat{}, nil).Once()
			ch1 <- zk.Event{
				Type: zk.EventNodeChildrenChanged,
				Path: test_zk_path,
			}
			time.Sleep(100 * time.Millisecond) // give async routines time to catch up
		}
		time.Sleep(1 * time.Second) // give async routines time to catch up
		t.Logf("disconnecting...")
		ch0 <- zk.Event{
			State: zk.StateDisconnected,
		}
		//TODO(jdef) does order of close matter here? probably, meaking client code is weak
		close(ch0)
		time.Sleep(500 * time.Millisecond) // give async routines time to catch up
		close(ch1)
	}()
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		wg.Wait()
	}()

	defer func() {
		if r := recover(); r != nil {
			t.Fatal(r)
		}
	}()

	select {
	case <-time.After(2 * time.Second):
		panic("timed out waiting for master changes to propagate")
	case <-completed:
	}
}

func TestMasterDetect_none(t *testing.T) {
	assert := assert.New(t)
	nodeList := []string{}
	node := selectTopNode(nodeList)
	assert.Equal("", node)
}
