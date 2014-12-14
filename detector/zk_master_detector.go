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
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/samuel/go-zookeeper/zk"
	"net/url"
	_ "time"
)

// ZkMasterDetector uses ZooKeeper to detect new leading master.
type ZkMasterDetector struct {
	zkPath    string
	zkHosts   []string
	zkConn    *zk.Conn
	zkConnCh  <-chan zk.Event
	zkEventCh <-chan zk.Event
	previous  *mesos.MasterInfo
	url       *url.URL
	pathLabel string
}

// Create a new ZkMasterDetector.
func NewZkMasterDetector(zkurls string) (*ZkMasterDetector, error) {
	url, err := url.Parse(zkurls)
	if err != nil {
		log.Fatalln("Failed to parse url", err)
		return nil, err
	}

	detector := new(ZkMasterDetector)
	detector.zkHosts = append(detector.zkHosts, url.Host)
	detector.zkPath = url.Path
	detector.pathLabel = "info"

	return detector, nil
}
