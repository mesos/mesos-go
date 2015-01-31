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

package detector

import (
	log "github.com/golang/glog"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/mock"
)

// Impersontates a zk.Connection
// It implements interface zkConnector
type MockZkConnector struct {
	mock.Mock
}

func NewMockZkConnector() *MockZkConnector {
	return new(MockZkConnector)
}

func (conn *MockZkConnector) Close() {
	conn.Called()
}

func (conn *MockZkConnector) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	log.V(2).Infoln("Invoking Mocked zk.Conn.ChidrenW")
	args := conn.Called(path)
	return args.Get(0).([]string),
		args.Get(1).(*zk.Stat),
		args.Get(2).(<-chan zk.Event),
		args.Error(3)
}

func (conn *MockZkConnector) Children(path string) ([]string, *zk.Stat, error) {
	args := conn.Called(path)
	return args.Get(0).([]string),
		args.Get(1).(*zk.Stat),
		args.Error(2)
}

func (conn *MockZkConnector) Get(path string) ([]byte, *zk.Stat, error) {
	args := conn.Called(path)
	return args.Get(0).([]byte),
		args.Get(1).(*zk.Stat),
		args.Error(2)
}
