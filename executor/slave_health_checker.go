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

package executor

import (
	"fmt"
	"net/http"
	"time"

	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/upid"
)

// SlaveHealthChecker is for checking the slave's health.
// TODO(yifan): Make it an interface?
type SlaveHealthChecker struct {
	slaveUPID                *upid.UPID
	threshold                int
	checkDuration            time.Duration
	continuousUnhealthyCount int
	stop                     chan struct{}
	C                        chan bool
}

// NewSlaveHealthChecker creates a slave health checker and return a notification channel.
// Each time the checker thinks the slave is unhealthy, it will send a notification through the channel.
func NewSlaveHealthChecker(slaveUPID *upid.UPID, threshold int, checkDuration time.Duration) *SlaveHealthChecker {
	checker := &SlaveHealthChecker{
		slaveUPID:     slaveUPID,
		threshold:     threshold,
		checkDuration: checkDuration,
		stop:          make(chan struct{}),
		C:             make(chan bool, 1),
	}
	go checker.start()
	return checker
}

// Stop stops the slave health checker.
func (c *SlaveHealthChecker) Stop() {
	close(c.stop)
}

func (c *SlaveHealthChecker) start() {
	ticker := time.Tick(c.checkDuration)
	for {
		select {
		case <-ticker:
			c.doCheck()
		case <-c.stop:
			return
		}
	}
}

func (c *SlaveHealthChecker) doCheck() {
	path := fmt.Sprintf("http://%s:%s/%s/health", c.slaveUPID.Host, c.slaveUPID.Port, c.slaveUPID.ID)
	resp, err := http.Get(path)
	if err != nil {
		log.Errorf("Failed to request the health path: %v\n", err)
		c.continuousUnhealthyCount++
		if c.continuousUnhealthyCount >= c.threshold {
			select {
			case c.C <- true: // If no one is receiving the channel, then just skip it.
			default:
			}
			c.continuousUnhealthyCount = 0
		}
		return
	}
	c.continuousUnhealthyCount = 0
	resp.Body.Close()
}
