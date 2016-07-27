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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseEtcdUrl_single(t *testing.T) {
	params, err := parseEtcdUrl("etcd://127.0.0.1:2379/mesos")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(params.hosts))
	assert.Equal(t, "/mesos", params.path)
}

func TestParseEtcdUrlWithUser(t *testing.T) {
	params, err := parseEtcdUrl("etcd://username:password@127.0.0.1:2379/mesos")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(params.hosts))
	assert.Equal(t, "/mesos", params.path)
	assert.Equal(t, "username", params.userName)
	assert.Equal(t, "password", params.password)
}

func TestParseEtcdUrl_multiHostname(t *testing.T) {
	params, err := parseEtcdUrl("etcd://grady1.host.com:2379,grady2.host.com:2379/mesos")

	assert.NoError(t, err)
	assert.Equal(t, []string{"grady1.host.com:2379", "grady2.host.com:2379"}, params.hosts)
	assert.Equal(t, "/mesos", params.path)
}

func TestParseEtcdUrl_multiIP(t *testing.T) {
	params, err := parseEtcdUrl("etcd://192.168.10.1:2379,192.168.10.2:2379,192.168.10.3:2379/mesos")

	assert.NoError(t, err)
	assert.Equal(t, []string{"192.168.10.1:2379", "192.168.10.2:2379", "192.168.10.3:2379"}, params.hosts)
	assert.Equal(t, "/mesos", params.path)
}
