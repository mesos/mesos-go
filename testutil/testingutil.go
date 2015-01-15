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

//Collection of resources for teting mesos artifacts.
package testutil

import (
	"bytes"
	"fmt"
	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/upid"
	"github.com/stretchr/testify/assert"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
)

//MockMesosHttpProcess represents a remote http process: master or slave.
type MockMesosHttpServer struct {
	PID    *upid.UPID
	Addr   string
	server *httptest.Server
	t      *testing.T
}

func NewMockMasterHttpServer(t *testing.T, handler func(rsp http.ResponseWriter, req *http.Request)) *MockMesosHttpServer {
	server := httptest.NewServer(http.HandlerFunc(handler))
	assert.NotNil(t, server)
	addr := server.Listener.Addr().String()
	pid, err := upid.Parse("master@" + addr)
	assert.NoError(t, err)
	assert.NotNil(t, pid)
	log.Infoln("Created test Master http server with PID", pid.String())
	return &MockMesosHttpServer{PID: pid, Addr: addr, server: server, t: t}
}

func NewMockSlaveHttpServer(t *testing.T, handler func(rsp http.ResponseWriter, req *http.Request)) *MockMesosHttpServer {
	server := httptest.NewServer(http.HandlerFunc(handler))
	assert.NotNil(t, server)
	addr := server.Listener.Addr().String()
	pid, err := upid.Parse("slave(1)@" + addr)
	assert.NoError(t, err)
	assert.NotNil(t, pid)
	assert.NoError(t, os.Setenv("MESOS_SLAVE_PID", pid.String()))
	assert.NoError(t, os.Setenv("MESOS_SLAVE_ID", "test-slave-001"))
	log.Infoln("Created test Slave http server with PID", pid.String())
	return &MockMesosHttpServer{PID: pid, Addr: addr, server: server, t: t}
}

func (s *MockMesosHttpServer) Close() {
	s.server.Close()
}

//MockMesosClient Http client to communicate with mesos processes (master,sched,exec)
type MockMesosClient struct {
	pid *upid.UPID
	t   *testing.T
}

func NewMockMesosClient(t *testing.T, pid *upid.UPID) *MockMesosClient {
	return &MockMesosClient{t: t, pid: pid}
}

// sendMessage Mocks sending event messages to a processes such as master, sched or exec.
func (c *MockMesosClient) SendMessage(targetPid *upid.UPID, message proto.Message) {
	if c.t == nil {
		panic("MockMesosClient needs a testing context.")
	}

	messageName := reflect.TypeOf(message).Elem().Name()
	data, err := proto.Marshal(message)
	assert.NoError(c.t, err)
	hostport := net.JoinHostPort(targetPid.Host, targetPid.Port)
	targetURL := fmt.Sprintf("http://%s/%s/mesos.internal.%s", hostport, targetPid.ID, messageName)
	log.Infoln("MockMesosClient Sending message to", targetURL)
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(data))
	assert.NoError(c.t, err)
	req.Header.Add("Libprocess-From", c.pid.String())
	req.Header.Add("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(c.t, err)
	assert.Equal(c.t, http.StatusAccepted, resp.StatusCode)
}
