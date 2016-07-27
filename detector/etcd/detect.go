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

package etcd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/detector"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

const (
	defaultMinDetectorCyclePeriod      = 1 * time.Second
	defaultEtcdHeaderTimeoutPerRequest = 3 * time.Second
)

type (
	ETCDInterface interface {
		Watch(string, uint64)
		Get(string) (*kvPair, error)
		Put(string, string) error
		Stop()
		MasterChangedChan() <-chan *kvPair
		MasterDetectErrChan() <-chan error
		Shutdown() <-chan struct{}
	}

	// Detector uses Etcd to detect new leading master.
	MasterDetector struct {
		// detection should not signal master change listeners more frequently than this
		cancel     func()
		done       chan struct{}
		client     ETCDInterface
		leaderNode string
		// latch: only install, at most, one ignoreChanged listener; see MasterDetector.Detect
		ignoreInstalled        int32
		masterDetectPath       string
		minDetectorCyclePeriod time.Duration
	}
)

type urlParams struct {
	hosts    []string
	path     string
	userName string
	password string
}

// reasonable default for a noop change listener
var ignoreChanged = detector.OnMasterChanged(func(*mesos.MasterInfo) {})

// Internal constructor function
func NewMasterDetector(etcdurls string, options ...detector.Option) (*MasterDetector, error) {
	params, err := parseEtcdUrl(etcdurls)

	if err != nil {
		log.Fatalln("Failed to parse url", err)
		return nil, err
	}

	clientOptions := &etcdClientCfg{
		connectionTimeout: defaultEtcdHeaderTimeoutPerRequest,
		username:          params.userName,
		password:          params.password,
	}

	client_, err := createEtcdClient(params.hosts, clientOptions)
	if err != nil {
		log.Fatalln("Failed to create etcd client.")
		return nil, err
	}

	masterDetector := &MasterDetector{
		leaderNode:             "",
		client:                 client_,
		cancel:                 client_.Stop,
		done:                   make(chan struct{}),
		masterDetectPath:       params.path,
		minDetectorCyclePeriod: defaultMinDetectorCyclePeriod,
	}

	// apply options last so that they can override default behavior
	for _, opt := range options {
		opt(masterDetector)
	}

	go func() {
		defer close(masterDetector.done)
		<-masterDetector.client.Shutdown()
	}()

	log.V(2).Infoln("Created new detector to watch", params.hosts, params.path)
	return masterDetector, nil
}

// call to Detect will add a changed listener. Detect may be called more than
// once, and each time the spec'd listener will be added to the list of those receiving notifications.
func (md *MasterDetector) Detect(f detector.MasterChanged) (err error) {
	if f == nil {
		// only ever install, at most, one ignoreChanged listener. multiple instances of it
		// just consume resources and generate misleading log messages.
		if !atomic.CompareAndSwapInt32(&md.ignoreInstalled, 0, 1) {
			log.V(3).Infoln("ignoreChanged listener already installed")
			return
		}

		f = ignoreChanged
	}

	log.V(3).Infoln("spawning detect()")
	go md.detect(f)
	return nil
}

func (md *MasterDetector) detect(f detector.MasterChanged) {
	log.V(3).Infoln("detecting children at", md.masterDetectPath)

	for {
		isDetectorClosed := false
		select {
		case <-md.Done():
			isDetectorClosed = true
		default:
		}

		if isDetectorClosed {
			break
		}

		started := time.Now()
		pair, err := md.client.Get(md.masterDetectPath)
		if err != nil {
			log.V(1).Infoln("Get the current master info with error, master lost; due to:", err.Error())
			md.notifyMasterChanged(nil, f)
			md.waitAfterDetected(started)
			continue
		}

		md.notifyMasterChanged(pair, f)

		go md.client.Watch(md.masterDetectPath, pair.lastIndex)
		isWatchFailed := false

		for {
			started := time.Now()
			select {
			case pairNode := <-md.client.MasterChangedChan():
				md.notifyMasterChanged(pairNode, f)
			case err := <-md.client.MasterDetectErrChan():
				log.V(1).Infoln("Watch ended with error, master lost; error was:", err)
				isWatchFailed = true
				md.notifyMasterChanged(nil, f)
			}
			if isWatchFailed {
				break
			}
			md.waitAfterDetected(started)
		}
	}
}

// Cancle closes the detector
func (md *MasterDetector) Cancel() {
	md.cancel()
}

// Done returns a chan that, when closed, indicates termination of the detector
func (md *MasterDetector) Done() <-chan struct{} {
	return md.done
}

// MinCyclePeriod is a functional option that determines the highest frequency of master change notifications
func MinCyclePeriod(d time.Duration) detector.Option {
	return func(di interface{}) detector.Option {
		md := di.(*MasterDetector)
		old := md.minDetectorCyclePeriod
		md.minDetectorCyclePeriod = d
		return MinCyclePeriod(old)
	}
}

func WithClient(usedClient ETCDInterface) detector.Option {
	return func(di interface{}) detector.Option {
		md := di.(*MasterDetector)
		old := md.client
		md.client = usedClient
		md.cancel = usedClient.Stop
		return WithClient(old)
	}
}

// waitAfterDetected defines the master detected cycle
func (md *MasterDetector) waitAfterDetected(started time.Time) {
	// rate-limit master changes
	if elapsed := time.Now().Sub(started); elapsed > 0 {
		log.V(2).Infoln("resting before next detection cycle")
		select {
		case <-md.Done():
			return
		case <-time.After(md.minDetectorCyclePeriod - elapsed): // noop
		}
	}
}

// notifyMasterChanged notifies the scheduler when Mesos master changed
func (md *MasterDetector) notifyMasterChanged(pair *kvPair, f detector.MasterChanged) {
	if pair == nil {
		md.leaderNode = ""
		logPanic(func() { f.OnMasterChanged(nil) })
		return
	}

	masterInfo := &mesos.MasterInfo{}

	if err := json.Unmarshal(pair.value, masterInfo); err != nil {
		log.Errorln("Unmarshal the master information %a failed, due to: ", string(pair.value), err.Error())
		md.leaderNode = ""
		logPanic(func() { f.OnMasterChanged(nil) })
	} else if md.leaderNode != *masterInfo.Hostname {
		log.V(2).Infof("changing leader node from %q -> %q", md.leaderNode, *masterInfo.Hostname)
		md.leaderNode = *masterInfo.Hostname
		logPanic(func() { f.OnMasterChanged(masterInfo) })
	}
}

// logPanic safely executes the given func, recovering from and logging a panic if one occurs.
func logPanic(f func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("recovered from client panic: %v", r)
		}
	}()
	f()
}

// parseEtcdUrl parses the etcd url
func parseEtcdUrl(etcdurls string) (*urlParams, error) {
	u, err := url.Parse(etcdurls)

	if err != nil {
		log.V(1).Infof("failed to parse url: %v", err)
		return nil, err
	}

	if u.Scheme != "etcd" {
		return nil, fmt.Errorf("invalid url scheme for etcd url: '%v'", u.Scheme)
	}

	var (
		username = ""
		password = ""
	)

	if u.User != nil {
		username = u.User.Username()
		passwd, _ := u.User.Password()
		password = passwd
	}

	return &urlParams{strings.Split(u.Host, ","), u.Path, username, password}, nil
}
