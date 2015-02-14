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
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/mesos/mesos-go/testutil"
	"github.com/stretchr/testify/assert"
)

// testScuduler is used for testing Schduler callbacks.
type testScheduler struct {
	ch chan bool
	wg *sync.WaitGroup
	s  *SchedulerTestSuite
}

// convenience
func (sched *testScheduler) T() *testing.T {
	return sched.s.T()
}

func (sched *testScheduler) Registered(dr SchedulerDriver, fw *mesos.FrameworkID, mi *mesos.MasterInfo) {
	log.Infoln("Sched.Registered() called.")
	sched.s.Equal(fw.GetValue(), sched.s.framework.Id.GetValue())
	sched.s.Equal(mi.GetIp(), uint32(123456))
	sched.ch <- true
}

func (sched *testScheduler) Reregistered(dr SchedulerDriver, mi *mesos.MasterInfo) {
	log.Infoln("Sched.Reregistered() called")
	sched.s.Equal(mi.GetIp(), uint32(123456))
	sched.ch <- true
}

func (sched *testScheduler) Disconnected(dr SchedulerDriver) {
	log.Infoln("Shed.Disconnected() called")
}

func (sched *testScheduler) ResourceOffers(dr SchedulerDriver, offers []*mesos.Offer) {
	log.Infoln("Sched.ResourceOffers called.")
	sched.s.NotNil(offers)
	sched.s.Equal(len(offers), 1)
	sched.ch <- true
}

func (sched *testScheduler) OfferRescinded(dr SchedulerDriver, oid *mesos.OfferID) {
	log.Infoln("Sched.OfferRescinded() called.")
	sched.s.NotNil(oid)
	sched.s.Equal("test-offer-001", oid.GetValue())
	sched.ch <- true
}

func (sched *testScheduler) StatusUpdate(dr SchedulerDriver, stat *mesos.TaskStatus) {
	log.Infoln("Sched.StatusUpdate() called.")
	sched.s.NotNil(stat)
	sched.s.Equal("test-task-001", stat.GetTaskId().GetValue())
	sched.wg.Done()
	log.Infof("Status update done with waitGroup %v \n", sched.wg)
}

func (sched *testScheduler) SlaveLost(dr SchedulerDriver, slaveId *mesos.SlaveID) {
	log.Infoln("Sched.SlaveLost() called.")
	sched.s.NotNil(slaveId)
	sched.s.Equal(slaveId.GetValue(), "test-slave-001")
	sched.ch <- true
}

func (sched *testScheduler) FrameworkMessage(dr SchedulerDriver, execId *mesos.ExecutorID, slaveId *mesos.SlaveID, data string) {
	log.Infoln("Sched.FrameworkMessage() called.")
	sched.s.NotNil(slaveId)
	sched.s.Equal(slaveId.GetValue(), "test-slave-001")
	sched.s.NotNil(execId)
	sched.s.NotNil(data)
	sched.s.Equal("test-data-999", string(data))
	sched.ch <- true
}

func (sched *testScheduler) ExecutorLost(SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
	log.Infoln("Sched.ExecutorLost	 called")
}

func (sched *testScheduler) Error(dr SchedulerDriver, err string) {
	log.Infoln("Sched.Error() called.")
	sched.s.Equal("test-error-999", err)
	sched.ch <- true
}

func (sched *testScheduler) waitForCallback(timeout time.Duration) bool {
	if timeout == 0 {
		timeout = 500 * time.Millisecond
	}
	select {
	case <-sched.ch:
		//callback complete
		return true
	case <-time.After(timeout):
		sched.T().Fatalf("timed out after waiting %v for callback", timeout)
	}
	return false
}

func newTestScheduler(s *SchedulerTestSuite) *testScheduler {
	return &testScheduler{ch: make(chan bool), s: s}
}

// ---------------------------------- Tests ---------------------------------- //

func (suite *SchedulerTestSuite) TestSchedulerDriverRegisterFrameworkMessage() {
	t := suite.T()

	id := suite.framework.Id
	suite.framework.Id = nil
	validated := make(chan struct{})
	server, _, _, ok := suite.newServerWithFramework(id, http.HandlerFunc(func(rsp http.ResponseWriter, req *http.Request) {
		defer close(validated)
		t.Logf("RCVD request ", req.URL)

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
		assert.Equal(t, suite.framework.GetName(), info.GetName())
		assert.True(t, reflect.DeepEqual(suite.framework.GetId(), info.GetId()))
		rsp.WriteHeader(http.StatusOK)
	}))
	if server != nil {
		defer server.Close()
	}
	assert.True(t, ok, "failed to establish running test server and driver")
	select {
	case <-time.After(1 * time.Second):
		t.Fatalf("failed to complete validation of framework registration message")
	case <-validated:
		// noop
	}
}

func (suite *SchedulerTestSuite) TestSchedulerDriverFrameworkRegisteredEvent() {
	t := suite.T()
	server, _, _, ok := suite.newServerWithRegisteredFramework()
	if server != nil {
		defer server.Close()
	}
	assert.True(t, ok, "failed to establish running test server and driver")
}

func (suite *SchedulerTestSuite) TestSchedulerDriverFrameworkReregisteredEvent() {
	t := suite.T()
	server, _, _, ok := suite.newServerWithFramework(suite.framework.Id, nil)
	if server != nil {
		defer server.Close()
	}
	assert.True(t, ok, "failed to establish running test server and driver")
}

func (suite *SchedulerTestSuite) newServerWithRegisteredFramework() (*testutil.MockMesosHttpServer, *MesosSchedulerDriver, *testScheduler, bool) {
	// suite.framework is used to initialize the FrameworkInfo of
	// the driver, so if we clear the Id then we'll expect a registration message
	id := suite.framework.Id
	suite.framework.Id = nil
	return suite.newServerWithFramework(id, nil)
}

// sets up a mock Mesos HTTP master listener, scheduler, and scheduler driver for testing.
func (suite *SchedulerTestSuite) newServerWithFramework(frameworkId *mesos.FrameworkID, validator http.HandlerFunc) (*testutil.MockMesosHttpServer, *MesosSchedulerDriver, *testScheduler, bool) {
	t := suite.T()
	// start mock master server to handle connection
	server := testutil.NewMockMasterHttpServer(t, func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		if validator != nil {
			validator(rsp, req)
		} else {
			ioutil.ReadAll(req.Body)
			defer req.Body.Close()
			rsp.WriteHeader(http.StatusAccepted)
		}
	})

	t.Logf("test HTTP server listening on %v", server.Addr)
	sched := newTestScheduler(suite)
	sched.ch = make(chan bool, 1)

	driver, err := NewMesosSchedulerDriver(sched, suite.framework, server.Addr, nil)
	assert.NoError(t, err)

	masterInfo := util.NewMasterInfo("master", 123456, 1234)
	server.On("/master/mesos.internal.RegisterFrameworkMessage").Do(func(rsp http.ResponseWriter, req *http.Request) {
		if validator != nil {
			t.Logf("validating registration request")
			validator(rsp, req)
		} else {
			ioutil.ReadAll(req.Body)
			defer req.Body.Close()
			rsp.WriteHeader(http.StatusAccepted)
		}
		// this is what the mocked scheduler is expecting to receive
		driver.frameworkRegistered(driver.MasterPid, &mesos.FrameworkRegisteredMessage{
			FrameworkId: frameworkId,
			MasterInfo:  masterInfo,
		})
	})
	server.On("/master/mesos.internal.ReregisterFrameworkMessage").Do(func(rsp http.ResponseWriter, req *http.Request) {
		if validator != nil {
			validator(rsp, req)
		} else {
			ioutil.ReadAll(req.Body)
			defer req.Body.Close()
			rsp.WriteHeader(http.StatusAccepted)
		}
		// this is what the mocked scheduler is expecting to receive
		driver.frameworkReregistered(driver.MasterPid, &mesos.FrameworkReregisteredMessage{
			FrameworkId: frameworkId,
			MasterInfo:  masterInfo,
		})
	})

	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	ok := waitForConnected(t, driver, 2*time.Second)
	if ok {
		ok = sched.waitForCallback(0) // registered or re-registered callback
	}
	return server, driver, sched, ok
}

func (suite *SchedulerTestSuite) TestSchedulerDriverResourceOffersEvent() {
	t := suite.T()
	server, driver, sched, ok := suite.newServerWithRegisteredFramework()
	if server != nil {
		defer server.Close()
	}
	assert.True(t, ok, "failed to establish running test server and driver")

	// Send a event to this SchedulerDriver (via http) to test handlers.
	offer := util.NewOffer(
		util.NewOfferID("test-offer-001"),
		suite.framework.Id,
		util.NewSlaveID("test-slave-001"),
		"test-localhost",
	)
	pbMsg := &mesos.ResourceOffersMessage{
		Offers: []*mesos.Offer{offer},
		Pids:   []string{"test-offer-001@test-slave-001:5051"},
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)
	sched.waitForCallback(0)
}

func (suite *SchedulerTestSuite) TestSchedulerDriverRescindOfferEvent() {
	t := suite.T()
	server, driver, sched, ok := suite.newServerWithRegisteredFramework()
	if server != nil {
		defer server.Close()
	}
	assert.True(t, ok, "failed to establish running test server and driver")

	// Send a event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.RescindResourceOfferMessage{
		OfferId: util.NewOfferID("test-offer-001"),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)
	sched.waitForCallback(0)
}

func (suite *SchedulerTestSuite) TestSchedulerDriverStatusUpdatedEvent() {
	t := suite.T()
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

	sched := newTestScheduler(suite)
	sched.wg = &wg

	driver, err := NewMesosSchedulerDriver(sched, suite.framework, server.Addr, nil)
	assert.NoError(t, err)
	stat, err := driver.Start()
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)
	assert.True(t, waitForConnected(t, driver, 2*time.Second))

	// Send a event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.StatusUpdateMessage{
		Update: util.NewStatusUpdate(
			suite.framework.Id,
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

func (suite *SchedulerTestSuite) TestSchedulerDriverLostSlaveEvent() {
	t := suite.T()
	server, driver, sched, ok := suite.newServerWithRegisteredFramework()
	if server != nil {
		defer server.Close()
	}
	assert.True(t, ok, "failed to establish running test server and driver")

	// Send a event to this SchedulerDriver (via http) to test handlers.	offer := util.NewOffer(
	pbMsg := &mesos.LostSlaveMessage{
		SlaveId: util.NewSlaveID("test-slave-001"),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)
	sched.waitForCallback(0)
}

func (suite *SchedulerTestSuite) TestSchedulerDriverFrameworkMessageEvent() {
	t := suite.T()
	server, driver, sched, ok := suite.newServerWithRegisteredFramework()
	if server != nil {
		defer server.Close()
	}
	assert.True(t, ok, "failed to establish running test server and driver")

	// Send a event to this SchedulerDriver (via http) to test handlers.	offer := util.NewOffer(
	pbMsg := &mesos.ExecutorToFrameworkMessage{
		SlaveId:     util.NewSlaveID("test-slave-001"),
		FrameworkId: suite.framework.Id,
		ExecutorId:  util.NewExecutorID("test-executor-001"),
		Data:        []byte("test-data-999"),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)
	sched.waitForCallback(0)
}

func waitForConnected(t *testing.T, driver *MesosSchedulerDriver, timeout time.Duration) bool {
	connected := make(chan struct{})
	go func() {
		defer close(connected)
		for !driver.Connected() {
			time.Sleep(200 * time.Millisecond)
		}
	}()
	select {
	case <-time.After(timeout):
		t.Fatalf("driver failed to establish connection within %v", timeout)
		return false
	case <-connected:
		return true
	}
}

func (suite *SchedulerTestSuite) TestSchedulerDriverFrameworkErrorEvent() {
	t := suite.T()
	server, driver, sched, ok := suite.newServerWithRegisteredFramework()
	if server != nil {
		defer server.Close()
	}
	assert.True(t, ok, "failed to establish running test server and driver")

	// Send an error event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.FrameworkErrorMessage{
		Message: proto.String("test-error-999"),
	}

	c := testutil.NewMockMesosClient(t, server.PID)
	c.SendMessage(driver.self, pbMsg)
	sched.waitForCallback(0)
	assert.Equal(t, mesos.Status_DRIVER_ABORTED, driver.Status())
}
