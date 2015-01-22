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

package zookeeper

import (
	"errors"
	"sort"
)

//ZkPath is an implementation of ZkNode.
//It represents a zookeeper path entry with data and possibly children nodes.
type ZkPath struct {
	Path     string
	ZkClient *ZkClientConnector
}

//NewZkPath creates a new ZkPath instance.
func NewZkPath(ZkClient *ZkClientConnector, path string) ZkNode {
	return &ZkPath{Path: path, ZkClient: ZkClient}
}

func (n *ZkPath) Data() ([]byte, error) {
	if !n.ZkClient.connected {
		return nil, errors.New("Unable to retrieve node data, client not connected.")
	}

	data, _, err := n.ZkClient.Conn.Get(n.Path)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (n *ZkPath) List() ([]ZkNode, error) {
	if !n.ZkClient.connected {
		return nil, errors.New("Unable to list node children, client not connected.")
	}

	children, _, err := n.ZkClient.Conn.Children(n.Path)
	if err != nil {
		return nil, err
	}

	// sort children (ascending).
	sort.Strings(children)

	list := make([]ZkNode, len(children))
	for i, child := range children {
		list[i] = NewZkPath(n.ZkClient, child)
	}
	return list, nil
}

func (n *ZkPath) String() string {
	return n.Path
}
