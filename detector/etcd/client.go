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
	etcd "github.com/coreos/etcd/client"
	log "github.com/golang/glog"
	"golang.org/x/net/context"
	"sync"
	"time"
)

// Etcd watch event actions
const (
	watchEventUpdate = "update"
	watchEventSet    = "set"
	watchEventCAS    = "compareAndSwap"
)

// Configurations for etcd client
type etcdClientCfg struct {
	connectionTimeout time.Duration
	username          string
	password          string
}

type etcdClient struct {
	client    etcd.KeysAPI
	etcdError chan error
	outgoing  chan *kvPair
	ctx       context.Context
	cancel    context.CancelFunc
	stopped   bool
	stopLock  sync.Mutex
	done      chan struct{} // signal chan, closes when the underlying connection terminates
}

type kvPair struct {
	key       string
	value     []byte
	lastIndex uint64
}

// Creates a new Etcd client with the specified list of endpoints and an optional configuration.
func createEtcdClient(addrs []string, options *etcdClientCfg) (*etcdClient, error) {
	entries := createEndpoints(addrs, "http")
	cfg := &etcd.Config{
		Endpoints:               entries,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: 3 * time.Second,
	}

	// Set options
	if options != nil {
		if options.connectionTimeout != 0 {
			setTimeout(cfg, options.connectionTimeout)
		}
		if options.username != "" {
			setCredentials(cfg, options.username, options.password)
		}
	}

	c, err := etcd.New(*cfg)
	if err != nil {
		log.Errorln(err.Error())
		return nil, err
	}

	s := &etcdClient{
		etcdError: make(chan error),
		outgoing:  make(chan *kvPair),
		client:    etcd.NewKeysAPI(c),
		done:      make(chan struct{}),
		stopped:   false,
		ctx:       nil,
		cancel:    nil,
	}

	go func() {
		defer close(s.etcdError)
		defer close(s.outgoing)
		<-s.done
	}()

	return s, nil
}

// watch watches the specified key
func (w *etcdClient) Watch(key string, watchIndex uint64) {
	var watcher etcd.Watcher

	w.stopLock.Lock()
	if w.stopped {
		// Watcher has already been stopped.
		w.stopLock.Unlock()
		return
	}

	opts := etcd.WatcherOptions{
		Recursive:  false,
		AfterIndex: watchIndex,
	}

	watcher = w.client.Watcher(key, &opts)
	w.ctx, w.cancel = context.WithCancel(context.TODO())

	w.stopLock.Unlock()

	for {
		result, err := watcher.Next(w.ctx)
		if err != nil {
			w.etcdError <- err
			break
		}

		switch result.Action {
		case watchEventUpdate, watchEventSet, watchEventCAS:
			w.outgoing <- &kvPair{
				key:       key,
				value:     []byte(result.Node.Value),
				lastIndex: result.Node.ModifiedIndex,
			}
		default:
			w.outgoing <- nil
		}
	}
}

// get gets the key-value pair of this specified key
func (w *etcdClient) Get(key string) (pair *kvPair, err error) {
	opts := etcd.GetOptions{
		Recursive: false,
		Sort:      false,
		Quorum:    true,
	}

	result, err := w.client.Get(context.TODO(), key, &opts)
	if err != nil {
		return nil, err
	}

	pair = &kvPair{
		key:       key,
		value:     []byte(result.Node.Value),
		lastIndex: result.Node.ModifiedIndex,
	}

	return pair, nil
}

// put adds a key-value pair into etcd cluster
func (w *etcdClient) Put(key string, value string) error {
	setOpts := &etcd.SetOptions{}
	_, err := w.client.Set(context.TODO(), key, value, setOpts)
	return err
}

// stop stops this client
func (w *etcdClient) Stop() {
	w.stopLock.Lock()
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
	if !w.stopped {
		w.stopped = true
	}
	w.stopLock.Unlock()

	if w.done != nil {
		close(w.done)
		w.done = nil
	}
}

// MasterChangedChan returns a chan to notify the caller when master changed
func (w *etcdClient) MasterChangedChan() <-chan *kvPair {
	return w.outgoing
}

// MasterDetectErrChan returns a chan to notify the caller when client detection error
func (w *etcdClient) MasterDetectErrChan() <-chan error {
	return w.etcdError
}

// shutdown returns a chan to notify the caller when this client is closed
func (w *etcdClient) Shutdown() <-chan struct{} {
	return w.done
}

// createEndpoints creates a list of endpoints given the right scheme
func createEndpoints(addrs []string, scheme string) (entries []string) {
	for _, addr := range addrs {
		entries = append(entries, scheme+"://"+addr)
	}
	return entries
}

// setTimeout sets the timeout used for connecting to etcd
func setTimeout(cfg *etcd.Config, time time.Duration) {
	cfg.HeaderTimeoutPerRequest = time
}

// setCredentials sets the username/password credentials for connecting to etcd
func setCredentials(cfg *etcd.Config, username, password string) {
	cfg.Username = username
	cfg.Password = password
}
