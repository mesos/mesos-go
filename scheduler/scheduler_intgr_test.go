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
	"bytes"
	"code.google.com/p/gogoprotobuf/proto"
	"fmt"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/upid"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func makeMockServer(handler func(rsp http.ResponseWriter, req *http.Request)) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(handler))
	log.Infoln("Created test http server  ", server.URL)
	return server
}

// MockMaster to send a event messages to processes.
func generateMasterEvent(t *testing.T, targetPid *upid.UPID, message proto.Message) {
	messageName := reflect.TypeOf(message).Elem().Name()
	data, err := proto.Marshal(message)
	assert.NoError(t, err)
	hostport := net.JoinHostPort(targetPid.Host, targetPid.Port)
	targetURL := fmt.Sprintf("http://%s/%s/mesos.internal.%s", hostport, targetPid.ID, messageName)
	log.Infoln("MockMaster Sending message to", targetURL)
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(data))
	assert.NoError(t, err)
	req.Header.Add("Libprocess-From", targetPid.String())
	req.Header.Add("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestSchedulerDriverRegisterFrameworkMessage(t *testing.T) {
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
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
	url, _ := url.Parse(server.URL)

	driver, err := NewMesosSchedulerDriver(&Scheduler{}, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.True(t, driver.stopped)

	stat := driver.Start()

	assert.False(t, driver.stopped)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, stat)

	<-time.After(time.Millisecond * 3)
}

func TestSchedulerDriverFrameworkRegisteredEvent(t *testing.T) {
	// start mock master server to handle connection
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()
	url, _ := url.Parse(server.URL)

	ch := make(chan bool)
	sched := &Scheduler{
		Registered: func(dr SchedulerDriver, fw *mesos.FrameworkID, mi *mesos.MasterInfo) {
			log.Infoln("Sched.Registered() called.")
			assert.Equal(t, fw.GetValue(), framework.Id.GetValue())
			assert.Equal(t, mi.GetIp(), 123456)
			ch <- true
		},
	}

	driver, err := NewMesosSchedulerDriver(sched, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Start())

	// Send a event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.FrameworkRegisteredMessage{
		FrameworkId: framework.Id,
		MasterInfo:  util.NewMasterInfo("master", 123456, 1234),
	}
	generateMasterEvent(t, driver.self, pbMsg) // after this driver.connced=true
	<-time.After(time.Millisecond * 1)
	assert.True(t, driver.connected)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverFrameworkReregisteredEvent(t *testing.T) {
	// start mock master server to handle connection
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()
	url, _ := url.Parse(server.URL)

	ch := make(chan bool)
	sched := &Scheduler{
		Reregistered: func(dr SchedulerDriver, mi *mesos.MasterInfo) {
			log.Infoln("Sched.Reregistered() called")
			assert.Equal(t, mi.GetIp(), 123456)
			ch <- true
		},
	}

	driver, err := NewMesosSchedulerDriver(sched, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Start())

	// Send a event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.FrameworkReregisteredMessage{
		FrameworkId: framework.Id,
		MasterInfo:  util.NewMasterInfo("master", 123456, 1234),
	}
	generateMasterEvent(t, driver.self, pbMsg)
	<-time.After(time.Millisecond * 1)
	assert.True(t, driver.connected)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverResourceOffersEvent(t *testing.T) {
	// start mock master server to handle connection
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()
	url, _ := url.Parse(server.URL)

	ch := make(chan bool)
	sched := &Scheduler{
		ResourceOffers: func(dr SchedulerDriver, offers []*mesos.Offer) {
			log.Infoln("Sched.ResourceOffers called.")
			assert.NotNil(t, offers)
			assert.Equal(t, len(offers), 1)
			ch <- true
		},
	}

	driver, err := NewMesosSchedulerDriver(sched, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Start())
	driver.connected = true // mock state

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
	generateMasterEvent(t, driver.self, pbMsg)
	<-time.After(time.Millisecond * 1)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverRescindOfferEvent(t *testing.T) {
	// start mock master server to handle connection
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()
	url, _ := url.Parse(server.URL)

	ch := make(chan bool)
	sched := &Scheduler{
		OfferRescinded: func(dr SchedulerDriver, oid *mesos.OfferID) {
			log.Infoln("Sched.OfferRescinded() called.")
			assert.NotNil(t, oid)
			assert.Equal(t, "test-offer-001", oid.GetValue())
			ch <- true
		},
	}

	driver, err := NewMesosSchedulerDriver(sched, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Start())
	driver.connected = true // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.
	pbMsg := &mesos.RescindResourceOfferMessage{
		OfferId: util.NewOfferID("test-offer-001"),
	}
	generateMasterEvent(t, driver.self, pbMsg)
	<-time.After(time.Millisecond * 1)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverStatusUpdatedEvent(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		if strings.Contains(req.RequestURI, "mesos.internal.StatusUpdateAcknowledgementMessage") {
			log.Infoln("Master cvd ACK")
			data, _ := ioutil.ReadAll(req.Body)
			defer req.Body.Close()
			assert.NotNil(t, data)
			wg.Done()
		}
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()
	url, _ := url.Parse(server.URL)

	//ch := make(chan bool)
	sched := &Scheduler{
		StatusUpdate: func(dr SchedulerDriver, stat *mesos.TaskStatus) {
			log.Infoln("Sched.StatusUpdate() called.")
			assert.NotNil(t, stat)
			assert.Equal(t, "test-task-001", stat.GetTaskId().GetValue())
			wg.Done()
		},
	}

	driver, err := NewMesosSchedulerDriver(sched, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Start())
	driver.connected = true // mock state

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
	generateMasterEvent(t, driver.self, pbMsg)
	<-time.After(time.Millisecond * 1)
	wg.Wait()
}

func TestSchedulerDriverLostSlaveEvent(t *testing.T) {
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()
	url, _ := url.Parse(server.URL)

	ch := make(chan bool)
	sched := &Scheduler{
		SlaveLost: func(dr SchedulerDriver, slaveId *mesos.SlaveID) {
			log.Infoln("Sched.SlaveLost() called.")
			assert.NotNil(t, slaveId)
			assert.Equal(t, slaveId.GetValue(), "test-slave-001")
			ch <- true
		},
	}

	driver, err := NewMesosSchedulerDriver(sched, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Start())
	driver.connected = true // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.	offer := util.NewOffer(
	pbMsg := &mesos.LostSlaveMessage{
		SlaveId: util.NewSlaveID("test-slave-001"),
	}
	generateMasterEvent(t, driver.self, pbMsg)
	<-time.After(time.Millisecond * 1)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverFrameworkMessageEvent(t *testing.T) {
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()
	url, _ := url.Parse(server.URL)

	ch := make(chan bool)
	sched := &Scheduler{
		FrameworkMessage: func(dr SchedulerDriver, execId *mesos.ExecutorID, slaveId *mesos.SlaveID, data []byte) {
			log.Infoln("Sched.FrameworkMessage() called.")
			assert.NotNil(t, slaveId)
			assert.Equal(t, slaveId.GetValue(), "test-slave-001")
			assert.NotNil(t, execId)
			assert.NotNil(t, data)
			assert.Equal(t, "test-data-999", string(data))
			ch <- true
		},
	}

	driver, err := NewMesosSchedulerDriver(sched, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Start())
	driver.connected = true // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.	offer := util.NewOffer(
	pbMsg := &mesos.ExecutorToFrameworkMessage{
		SlaveId:     util.NewSlaveID("test-slave-001"),
		FrameworkId: framework.Id,
		ExecutorId:  util.NewExecutorID("test-executor-001"),
		Data:        []byte("test-data-999"),
	}
	generateMasterEvent(t, driver.self, pbMsg)
	<-time.After(time.Millisecond * 1)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}
}

func TestSchedulerDriverFrameworkErrorEvent(t *testing.T) {
	server := makeMockServer(func(rsp http.ResponseWriter, req *http.Request) {
		log.Infoln("MockMaster - rcvd ", req.RequestURI)
		rsp.WriteHeader(http.StatusAccepted)
	})

	defer server.Close()
	url, _ := url.Parse(server.URL)

	ch := make(chan bool)
	sched := &Scheduler{
		Error: func(dr SchedulerDriver, err string) {
			log.Infoln("Sched.Error() called.")
			assert.Equal(t, "test-error-999", err)
			ch <- true
		},
	}

	driver, err := NewMesosSchedulerDriver(sched, framework, url.Host, nil)
	assert.NoError(t, err)
	assert.Equal(t, mesos.Status_DRIVER_RUNNING, driver.Start())
	driver.connected = true // mock state

	// Send a event to this SchedulerDriver (via http) to test handlers.	offer := util.NewOffer(
	pbMsg := &mesos.FrameworkErrorMessage{
		Message: proto.String("test-error-999"),
	}
	generateMasterEvent(t, driver.self, pbMsg)
	<-time.After(time.Millisecond * 1)
	select {
	case <-ch:
	case <-time.After(time.Millisecond * 2):
		log.Errorf("Tired of waiting for scheduler callback.")
	}

	assert.Equal(t, mesos.Status_DRIVER_ABORTED, driver.status)
}
