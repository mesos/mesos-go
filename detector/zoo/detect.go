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

package zoo

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/detector"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

const (
	// prefix for nodes listed at the ZK URL path
	nodePrefix = "info_"
)

// reasonable default for a noop change listener
var ignoreChanged = detector.AsMasterChanged(func(*mesos.MasterInfo) {})

// Detector uses ZooKeeper to detect new leading master.
type MasterDetector struct {
	zkPath     string
	zkHosts    []string
	client     *Client
	leaderNode string
	url        *url.URL
	obs        detector.MasterChanged
}

// Internal constructor function
func NewMasterDetector(zkurls string) (*MasterDetector, error) {
	u, err := url.Parse(zkurls)
	if err != nil {
		log.Fatalln("Failed to parse url", err)
		return nil, err
	}

	detector := &MasterDetector{
		url:     u,
		zkHosts: []string{u.Host}, //TODO(jdef) support multiple hosts
		zkPath:  u.Path,
		obs:     ignoreChanged,
	}

	detector.client, err = newClient(detector.zkHosts, detector.zkPath)
	if err != nil {
		return nil, err
	}

	log.V(2).Infoln("Created new detector, watching ", detector.zkHosts, detector.zkPath)
	return detector, nil
}

func (md *MasterDetector) Start() error {
	if err := md.client.connect(); err != nil {
		return err
	}
	return nil
}

func (md *MasterDetector) Stop() error {
	return nil
}

//TODO(jdef) execute async because we don't want to stall our client's event loop
func (md *MasterDetector) childrenChanged(zkc *Client, path string) {
	list, err := zkc.list(path)
	if err != nil {
		return
	}

	topNode := selectTopNode(list)

	if md.leaderNode == topNode {
		log.V(2).Infof("Ignoring children-changed event %s, leader has not changed.", path)
		return
	}

	log.V(2).Infof("Changing leader node from %s -> %s\n", md.leaderNode, topNode)
	md.leaderNode = topNode

	data, err := zkc.data(path)
	if err != nil {
		log.Errorln("Unable to retrieve leader data:", err.Error())
		return
	}
	masterInfo := new(mesos.MasterInfo)
	err = proto.Unmarshal(data, masterInfo)
	if err != nil {
		log.Errorln("Unable to unmarshall MasterInfo data from zookeeper.")
		return
	}

	if md.obs != nil {
		md.obs.Notify(masterInfo)
	}
}

func (md *MasterDetector) Detect(f detector.MasterChanged) error {
	if f == nil {
		f = ignoreChanged
	}
	md.obs = f

	log.V(2).Infoln("Detect function installed.")

	watchEnded, err := md.client.watchChildren(currentPath, ChildWatcher(md.childrenChanged))
	if err != nil {
		return err
	}
	go func() {
		select {
		case <-watchEnded:
			f.Notify(nil)
		}
	}()
	return nil
}

func selectTopNode(list []string) string {
	var leaderSeq uint64 = math.MaxUint64

	for _, v := range list {
		seqStr := strings.TrimPrefix(v, nodePrefix)
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			log.Warningf("unexpected zk node format '%s': %v", seqStr, err)
			continue
		}
		if seq < leaderSeq {
			leaderSeq = seq
		}
	}

	if leaderSeq == math.MaxUint64 {
		log.V(3).Infoln("No top node found.")
		return ""
	}

	node := fmt.Sprintf("%s%d", nodePrefix, leaderSeq)
	log.V(3).Infof("Top node selected: '%s'", node)
	return node
}
