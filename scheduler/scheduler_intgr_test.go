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

package scheduler

import (
	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/testutil"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// testScuduler is used for testing Schduler callbacks.
type testScheduler struct {
	ch chan bool
	wg *sync.WaitGroup
	t  *testing.T
}

func (sched *testScheduler) Registered(dr SchedulerDriver, fw *mesos.FrameworkID, mi *mesos.MasterInfo) {
	log.Infoln("Sched.Registered() called.")
	assert.Equal(sched.t, fw.GetValue(), framework.Id.GetValue())
	assert.Equal(sched.t, mi.GetIp(), uint32(123456))
	sched.ch <- true
}

func (sched *testScheduler) Reregistered(dr SchedulerDriver, mi *mesos.MasterInfo) {
	log.Infoln("Sched.Reregistered() called")
	assert.Equal(sched.t, mi.GetIp(), uint32(123456))
	sched.ch <- true
}

func (sched *testScheduler) Disconnected(dr SchedulerDriver) {
	log.Infoln("Shed.Disconnected() called")
}

func (sched *testScheduler) ResourceOffers(dr SchedulerDriver, offers []*mesos.Offer) {
	log.Infoln("Sched.ResourceOffers called.")
	assert.NotNil(sched.t, offers)
	assert.Equal(sched.t, len(offers), 1)
	sched.ch <- true
}

func (sched *testScheduler) OfferRescinded(dr SchedulerDriver, oid *mesos.OfferID) {
	log.Infoln("Sched.OfferRescinded() called.")
	assert.NotNil(sched.t, oid)
	assert.Equal(sched.t, "test-offer-001", oid.GetValue())
	sched.ch <- true
}

func (sched *testScheduler) StatusUpdate(dr SchedulerDriver, stat *mesos.TaskStatus) {
	log.Infoln("Sched.StatusUpdate() called.")
	assert.NotNil(sched.t, stat)
	assert.Equal(sched.t, "test-task-001", stat.GetTaskId().GetValue())
	sched.wg.Done()
	log.Infof("Status update done with waitGroup %v \n", sched.wg)
}

func (sched *testScheduler) SlaveLost(dr SchedulerDriver, slaveId *mesos.SlaveID) {
	log.Infoln("Sched.SlaveLost() called.")
	assert.NotNil(sched.t, slaveId)
	assert.Equal(sched.t, slaveId.GetValue(), "test-slave-001")
	sched.ch <- true
}

func (sched *testScheduler) FrameworkMessage(dr SchedulerDriver, execId *mesos.ExecutorID, slaveId *mesos.SlaveID, data string) {
	log.Infoln("Sched.FrameworkMessage() called.")
	assert.NotNil(sched.t, slaveId)
	assert.Equal(sched.t, slaveId.GetValue(), "test-slave-001")
	assert.NotNil(sched.t, execId)
	assert.NotNil(sched.t, data)
	assert.Equal(sched.t, "test-data-999", string(data))
	sched.ch <- true
}

func (sched *testScheduler) ExecutorLost(SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
	log.Infoln("Sched.ExecutorLost	 called")
}

func (sched *testScheduler) Error(dr SchedulerDriver, err string) {
	log.Infoln("Sched.Error() called.")
	assert.Equal(sched.t, "test-error-999", err)
	sched.ch <- true
}

func newTestScheduler() *testScheduler {
	return &testScheduler{ch: make(chan bool)}
}

// ---------------------------------- Tests ---------------------------------- //

func TestSchedulerDriverRegisterFrameworkMessage(t *testing.T) {
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("RCVD request ", req.URL)

		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("Missing RegisteredFramework data from scheduler.")
		}
		defer req.Body.Close()

		message := new(mesos.RegisterFrameworkMessage)
		err = proto.Unmarshal(data, message)
		if err != nil {
			t.Fatal("Problem unmarshaling expected RegisterFrameworkMessage")
		}

		assert.NotNil(t, message)
		info := message.GetFramework()
		assert.NotNil(t, info)
		assert.Equal(t, framework.GetName(), info.GetName())
		assert.Equal(t, framework.GetId().GetValue(), info.GetId().GetValue())
		rsp.WriteHeader(http.StatusOK)
	})
	defer server.Close()

	driver, err := NewMesosSchedulerDriver(NewMockScheduler(), framework, server.Addr, nil)
	assert.NoError(t, err)
	assert.True(t, driver.Stopped())

	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.False(t, driver.Stopped())
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)

	<-time.After(time.Millisecond * 3)
}

func TestSchedulerDriverFrameworkRegisteredEvent(t *testing.T) {
	// start mock master server to handle connection
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()

	ch := make(chan bool)
	sched := newTestScheduler()
	sched.ch = ch
	sched.t = t

	driver, err := NewMesosSchedulerDriver(sched, framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)

	// Send an event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.FrameworkRegisteredMessage{
		FrameworkId: framework.Id,
		MasterInfo:  util.NewMasterInfo("master", 123456, 1234),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg) // after this driver.connced=true

	assert.True(t, driver.Connected())
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverFrameworkReregisteredEvent(t *testing.T) {
	// start mock master server to handle connection
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})
	defer server.Close()

	ch := make(chan bool)
	sched := newTestScheduler()
	sched.ch = ch
	sched.t = t

	driver, err := NewMesosSchedulerDriver(sched, framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)

	// Send a event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.FrameworkReregisteredMessage{
		FrameworkId: framework.Id,
		MasterInfo:  util.NewMasterInfo("master", 123456, 1234),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)

	assert.True(t, driver.Connected())

	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverResourceOffersEvent(t *testing.T) {
	// start mock master server to handle connection
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()

	ch := make(chan bool)
	sched := newTestScheduler()
	sched.ch = ch
	sched.t = t

	driver, err := NewMesosSchedulerDriver(sched, framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	driver.setConnected(true) // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.
	offer := util.NewOffer(
		util.NewOfferID("test-offer-001"),
		framework.Id,
		util.NewSlaveID("test-slave-001"),
		"test-localhost",
	)
	pbMsg := &mesos.ResourceOffersMessage{
		Offers: []*mesos.Offer{offer},
		Pids:   []string{"test-offer-001"},
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)

	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverRescindOfferEvent(t *testing.T) {
	// start mock master server to handle connection
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()

	ch := make(chan bool)
	sched := newTestScheduler()
	sched.ch = ch
	sched.t = t

	driver, err := NewMesosSchedulerDriver(sched, framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	driver.setConnected(true) // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.RescindResourceOfferMessage{
		OfferId: util.NewOfferID("test-offer-001"),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)

	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverStatusUpdatedEvent(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		if strings.Contains(req.RequestURI, "mesos.internal.StatusUpdateAcknowledgementMessage") {
			log.Infoln("Master cvd ACK")
			data, _ := ioutil.ReadAll(req.Body)
			defer req.Body.Close()
			assert.NotNil(t, data)
			wg.Done()
			log.Infof("MockMaster - Done with wait group %v \n", wg)
		}
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()

	sched := newTestScheduler()
	sched.wg = &wg
	sched.t = t

	driver, err := NewMesosSchedulerDriver(sched, framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	driver.setConnected(true) // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.StatusUpdateMessage{
		Update: util.NewStatusUpdate(
			framework.Id,
			util.NewTaskStatus(util.NewTaskID("test-task-001"), mesos.TaskState_TASK_STARTING),
			float64(time.Now().Unix()),
			[]byte("test-abcd-ef-3455-454-001"),
		),
		Pid: proto.String(driver.self.String()),
	}
	pbMsg.Update.SlaveId = &mesos.SlaveID{Value: proto.String("test-slave-001")}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)

	<-time.After(time.Millisecond * 1)
	wg.Wait()
}

func TestSchedulerDriverLostSlaveEvent(t *testing.T) {
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()

	ch := make(chan bool)
	sched := newTestScheduler()
	sched.ch = ch
	sched.t = t

	driver, err := NewMesosSchedulerDriver(sched, framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	driver.setConnected(true) // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.	offer := util.NewOffer(
	pbMsg := &mesos.LostSlaveMessage{
		SlaveId: util.NewSlaveID("test-slave-001"),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)

	<-time.After(time.Millisecond * 1)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverFrameworkMessageEvent(t *testing.T) {
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()

	ch := make(chan bool)
	sched := newTestScheduler()
	sched.ch = ch
	sched.t = t

	driver, err := NewMesosSchedulerDriver(sched, framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	driver.setConnected(true) // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.	offer := util.NewOffer(
	pbMsg := &mesos.ExecutorToFrameworkMessage{
		SlaveId:     util.NewSlaveID("test-slave-001"),
		FrameworkId: framework.Id,
		ExecutorId:  util.NewExecutorID("test-executor-001"),
		Data:        []byte("test-data-999"),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)

	<-time.After(time.Millisecond * 1)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverFrameworkErrorEvent(t *testing.T) {
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()

	ch := make(chan bool)
	sched := newTestScheduler()
	sched.ch = ch
	sched.t = t

	driver, err := NewMesosSchedulerDriver(sched, framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	driver.setConnected(true) // mock state
	driver.setStopped(false)

	// Send an error event to this SchedulerDriver (via http) to test handlers.	offer := util.NewOffer(
	pbMsg := &mesos.FrameworkErrorMessage{
		Message: proto.String("test-error-999"),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)

	<-time.After(time.Millisecond * 1)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}

	assert.Equal(t, mesos.Status_DRIVER_ABORTED, driver.Status())
}
