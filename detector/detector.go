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
	mesos "github.com/mesos/mesos-go/mesosproto"
)

// An abstraction of a Master detector which can be used to
// detect the leading master from a group.
type MasterDetector interface {
	// Detect new master election. Every tiem a new master is
	// elected, the MasterInfo will be send back through the
	// receiver channel.
	//
	// If it fails to start detection, then an error is returned.
	//
	// If some error happends during detection, then the receiver
	// channel will be closed.
	Detect(receiver chan *mesos.MasterInfo)

	// Stop detection.
	Stop()
}
