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

//ZkDataNode is an implementation of ZkNode.
//It represents a zookeeper path entry with data and possibly children nodes.
type ZkDataNode struct {
	Path     string
	zkClient *ZkClientObject
}

//NewZkDataNode creates a new ZkDataNode instance.
func NewZkDataNode(zkClient *ZkClientObject, path string) ZkNode {
	return &ZkDataNode{Path: path, zkClient: zkClient}
}

func (n *ZkDataNode) Data() ([]byte, error) {
	if !n.zkClient.connected {
		return nil, errors.New("Unable to retrieve node data, client not connected.")
	}

	data, _, err := n.zkClient.Conn.Get(n.Path)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (n *ZkDataNode) List() ([]ZkNode, error) {
	if !n.zkClient.connected {
		return nil, errors.New("Unable to list node children, client not connected.")
	}

	children, _, err := n.zkClient.Conn.Children(n.Path)
	if err != nil {
		return nil, err
	}

	// sort children (ascending).
	sort.Strings(children)

	list := make([]ZkNode, len(children))
	for i, child := range children {
		list[i] = NewZkDataNode(n.zkClient, child)
	}
	return list, nil
}

func (n *ZkDataNode) String() string {
	return n.Path
}
