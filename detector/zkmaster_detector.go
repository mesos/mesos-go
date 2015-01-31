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
	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"math"
	"net/url"
	"strconv"
	"strings"
)

// ZkMasterDetector uses ZooKeeper to detect new leading master.
type ZkMasterDetector struct {
	zkPath       string
	zkHosts      []string
	zkClient     *zkClient
	leaderNode   string
	url          *url.URL
	nodePrefix   string
	detectedFunc func(*mesos.MasterInfo)
}

// Internal constructor function
func NewZkMasterDetector(zkurls string) (*ZkMasterDetector, error) {
	u, err := url.Parse(zkurls)
	if err != nil {
		log.Fatalln("Failed to parse url", err)
		return nil, err
	}

	detector := new(ZkMasterDetector)
	detector.url = u
	detector.zkHosts = append(detector.zkHosts, u.Host)
	detector.zkPath = u.Path
	detector.nodePrefix = "info_"

	detector.zkClient, err = newZkClient(detector.zkHosts, detector.zkPath)
	if err != nil {
		return nil, err
	}

	detector.zkClient.childrenWatcher = zkChildrenWatcherFunc(detector.childrenChanged)

	log.V(2).Infoln("Created new detector, watching ", detector.zkHosts, detector.zkPath)
	return detector, nil
}

func (md *ZkMasterDetector) Start() error {
	if err := md.zkClient.connect(); err != nil {
		return err
	}
	return nil
}

func (md *ZkMasterDetector) Stop() error {
	return nil
}

func (md *ZkMasterDetector) childrenChanged(zkc *zkClient, path string) {
	list, err := zkc.list(path)
	if err != nil {
		return
	}

	topNode := md.selectTopNode(list)

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

	if md.detectedFunc != nil {
		md.detectedFunc(masterInfo)
	}
}

func (md *ZkMasterDetector) Detect(f func(*mesos.MasterInfo)) error {
	log.V(2).Infoln("Detect function installed.")
	md.detectedFunc = f

	err := md.zkClient.watchChildren(".") // watch the current path (speci)
	if err != nil {
		return err
	}

	return nil
}

func (md *ZkMasterDetector) selectTopNode(list []string) string {
	var leaderSeq uint64 = math.MaxUint64

	for _, v := range list {
		seqStr := strings.TrimPrefix(v, md.nodePrefix)
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue
		}

		if seq < leaderSeq {
			leaderSeq = seq
		}
	}

	if leaderSeq == math.MaxInt64 {
		log.V(3).Infoln("No top node found.")
		return ""
	}

	node := md.nodePrefix + strconv.FormatUint(leaderSeq, 10)
	log.V(3).Infoln("Top node selected: ", node)
	return node
}
