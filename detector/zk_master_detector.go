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

// import (
// 	"code.google.com/p/gogoprotobuf/proto"
// 	log "github.com/golang/glog"
// 	mesos "github.com/mesos/mesos-go/mesosproto"
// 	"github.com/mesos/mesos-go/zookeeper"

// 	"net/url"
// )

// // ------------------ MasterWatcher ------------------- //
// type masterWatcher struct {
// 	detector *ZkMasterDetector
// }

// func newMasterWatcher(md *ZkMasterDetector) *masterWatcher {
// 	return &masterWatcher{detector: md}
// }

// func (w *masterWatcher) Connected(zkClient zookeeper.ZkClient) {

// }

// func (w *masterWatcher) ChildrenChanged(c zookeeper.ZkClient, node zookeeper.ZkNode) {
// 	list, err := node.List()
// 	if err != nil {
// 		log.Errorf("Unable to retrieve children list for %s\n", node.String())
// 		return
// 	}
// 	if len(list) == 0 {
// 		log.Errorf("Node %s has no children\n", node.String())
// 		return
// 	}

// 	// list is sorted (ascending).  So, first element is leader.
// 	leaderNode := list[0]
// 	if w.detector.leaderNode != nil && w.detector.leaderNode.String() == leaderNode.String() {
// 		log.V(2).Infof("Ignoring ChildrenChanged event for node %s, leader has not changed.", node.String())
// 		return
// 	}

// 	w.detector.leaderNode = leaderNode
// 	data, err := leaderNode.Data()
// 	if err != nil {
// 		log.Errorln("Unable to retrieve leader data:", err.Error())
// 		return
// 	}
// 	masterInfo := new(mesos.MasterInfo)
// 	err = proto.Unmarshal(data, masterInfo)
// 	if err != nil {
// 		log.Errorln("Unable to unmarshall MasterInfo data from zookeeper.")
// 		return
// 	}

// 	if w.detector.detectedFunc != nil {
// 		w.detector.detectedFunc(masterInfo)
// 	}
// }

// func (w *masterWatcher) Error(err error) {

// }

// // ZkMasterDetector uses ZooKeeper to detect new leading master.
// type ZkMasterDetector struct {
// 	zkPath       string
// 	zkHosts      []string
// 	zkClient     *zookeeper.ZkClientConnector
// 	leaderNode   zookeeper.ZkNode
// 	url          *url.URL
// 	nodePrefix   string
// 	detectedFunc func(*mesos.MasterInfo)
// }

// // Internal constructor function
// func NewZkMasterDetector(zkurls string) (*ZkMasterDetector, error) {
// 	u, err := url.Parse(zkurls)
// 	if err != nil {
// 		log.Fatalln("Failed to parse url", err)
// 		return nil, err
// 	}

// 	detector := new(ZkMasterDetector)
// 	detector.url = u
// 	detector.zkHosts = append(detector.zkHosts, u.Host)
// 	detector.zkPath = u.Path
// 	detector.nodePrefix = "info_"
// 	detector.zkClient = zookeeper.NewZkClientConnector()

// 	log.V(2).Infoln("Created new detector, watching ", detector.zkHosts, detector.zkPath)
// 	return detector, nil
// }

// func (md *ZkMasterDetector) Detect(f func(*mesos.MasterInfo)) error {
// 	md.detectedFunc = f

// 	err := md.zkClient.Connect(md.zkHosts, md.zkPath, newMasterWatcher(md))
// 	if err != nil {
// 		return err
// 	}
// 	err = md.zkClient.WatchChildren(".") // watch the current path (speci)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }
