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

package healthchecker

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mesos/mesos-go/upid"
	"github.com/mesosphere/testify/assert"
)

type slave struct {
	Cnt       int
	threshold int
	ts        *httptest.Server
}

func startSlave(threshold int) *slave {
	s := &slave{threshold: threshold}
	s.ts = httptest.NewServer(s)
	return s
}

func (s *slave) handleFunc(w http.ResponseWriter, r *http.Request) {
	s.Cnt++
	if s.Cnt <= s.threshold {
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic("Cannot hijack!")
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		panic("Cannot hijack")
	}
	conn.Close()
}

func (s *slave) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handleFunc(w, r)
}

func (s *slave) closeSlave() {
	s.ts.Close()
}

func TestSlaveHealthCheckerFailed(t *testing.T) {
	upid := &upid.UPID{ID: "slave", Host: "127.0.0.1", Port: "8000"}
	slave := startSlave(0)
	checker := NewSlaveHealthChecker(upid, 10, time.Second)

	select {
	case <-time.After(time.Second * 10):
		t.Fatal("time out")
	case <-checker.C:
		assert.Equal(t, 10, slave.Cnt)
	}
}
