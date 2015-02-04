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

package messenger

import (
	"flag"
	"fmt"
	"reflect"
	"time"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/upid"
	"golang.org/x/net/context"
)

const (
	defaultQueueSize = 1024
	preparePeriod    = time.Second * 1
)

var (
	sendRoutines   int
	encodeRoutines int
	decodeRoutines int
)

func init() {
	flag.IntVar(&sendRoutines, "send-routines", 1, "Number of network sending routines")
	flag.IntVar(&encodeRoutines, "encode-routines", 1, "Number of encoding routines")
	flag.IntVar(&decodeRoutines, "decode-routines", 1, "Number of decoding routines")
}

// MessageHandler is the callback of the message. When the callback
// is invoked, the sender's upid and the message is passed to the callback.
type MessageHandler func(from *upid.UPID, pbMsg proto.Message)

// Messenger defines the interfaces that should be implemented.
type Messenger interface {
	Install(handler MessageHandler, msg proto.Message) error
	Send(ctx context.Context, upid *upid.UPID, msg proto.Message) error
	Route(ctx context.Context, from *upid.UPID, msg proto.Message) error
	Start() error
	Stop() error
	UPID() *upid.UPID
}

// MesosMessenger is an implementation of the Messenger interface.
type MesosMessenger struct {
	upid              *upid.UPID
	encodingQueue     chan *Message
	sendingQueue      chan *Message
	installedMessages map[string]reflect.Type
	installedHandlers map[string]MessageHandler
	stop              chan struct{}
	tr                Transporter
}

// NewMesosMessenger creates a new mesos messenger.
func NewHttp(upid *upid.UPID) *MesosMessenger {
	return New(upid, NewHTTPTransporter(upid))
}

func New(upid *upid.UPID, t Transporter) *MesosMessenger {
	return &MesosMessenger{
		upid:              upid,
		encodingQueue:     make(chan *Message, defaultQueueSize),
		sendingQueue:      make(chan *Message, defaultQueueSize),
		installedMessages: make(map[string]reflect.Type),
		installedHandlers: make(map[string]MessageHandler),
		tr:                t,
	}
}

/// Install installs the handler with the given message.
func (m *MesosMessenger) Install(handler MessageHandler, msg proto.Message) error {
	// Check if the message is a pointer.
	mtype := reflect.TypeOf(msg)
	if mtype.Kind() != reflect.Ptr {
		return fmt.Errorf("Message %v is not a Ptr type")
	}

	// Check if the message is already installed.
	name := getMessageName(msg)
	if _, ok := m.installedMessages[name]; ok {
		return fmt.Errorf("Message %v is already installed", name)
	}
	m.installedMessages[name] = mtype.Elem()
	m.installedHandlers[name] = handler
	m.tr.Install(name)
	return nil
}

// Send puts a message into the outgoing queue, waiting to be sent.
// With buffered channels, this will not block under moderate throughput.
// When an error is generated, the error can be communicated by placing
// a message on the incoming queue to be handled upstream.
func (m *MesosMessenger) Send(ctx context.Context, upid *upid.UPID, msg proto.Message) error {
	if upid.Equal(m.upid) {
		return fmt.Errorf("Send the message to self")
	}
	name := getMessageName(msg)
	log.V(2).Infof("Sending message %v to %v\n", name, upid)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.encodingQueue <- &Message{upid, name, msg, nil}:
		return nil
	}
}

// Route puts a message either in the incoming or outgoing queue.
// This method is useful for:
// 1) routing internal error to callback handlers
// 2) testing components without starting remote servers.
func (m *MesosMessenger) Route(ctx context.Context, upid *upid.UPID, msg proto.Message) error {
	// if destination is not self, send to outbound.
	if !upid.Equal(m.upid) {
		return m.Send(ctx, upid, msg)
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	name := getMessageName(msg)
	return m.tr.Inject(ctx, &Message{upid, name, msg, data})
}

// Start starts the messenger.
func (m *MesosMessenger) Start() error {
	if err := m.tr.Listen(); err != nil {
		log.Errorf("Failed to start messenger: %v\n", err)
		return err
	}
	m.upid = m.tr.UPID()

	m.stop = make(chan struct{})
	errChan := make(chan error)
	go func() {
		if err := m.tr.Start(); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-time.After(preparePeriod):
	}
	for i := 0; i < sendRoutines; i++ {
		go m.sendLoop()
	}
	for i := 0; i < encodeRoutines; i++ {
		go m.encodeLoop()
	}
	for i := 0; i < decodeRoutines; i++ {
		go m.decodeLoop()
	}
	return nil
}

// Stop stops the messenger and clean up all the goroutines.
func (m *MesosMessenger) Stop() error {
	if err := m.tr.Stop(); err != nil {
		log.Errorf("Failed to stop the transporter: %v\n", err)
		return err
	}
	close(m.stop)
	return nil
}

// UPID returns the upid of the messenger.
func (m *MesosMessenger) UPID() *upid.UPID {
	return m.upid
}

func (m *MesosMessenger) encodeLoop() {
	for {
		select {
		case <-m.stop:
			return
		case msg := <-m.encodingQueue:
			e := func() error {
				//TODO(jdef) implement timeout for context
				ctx, cancel := context.WithCancel(context.TODO())
				defer cancel()

				b, err := proto.Marshal(msg.ProtoMessage)
				if err != nil {
					return err
				}
				msg.Bytes = b
				select {
				case <-ctx.Done():
					return ctx.Err()
				case m.sendingQueue <- msg:
					return nil
				}
			}()
			if e != nil {
				m.reportError(fmt.Errorf("Failed to enqueue message %v: %v", msg, e))
			}
		}
	}
}

func (m *MesosMessenger) reportError(err error) {
	log.V(2).Info(err)
	//TODO(jdef) implement timeout for context
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	c := make(chan error, 1)
	go func() { c <- m.Route(ctx, m.UPID(), &mesos.FrameworkErrorMessage{Message: proto.String(err.Error())}) }()
	select {
	case <-ctx.Done():
		<-c // wait for Route to return
	case e := <-c:
		log.Errorf("failed to report error %v due to: %v", err, e)
	}
}

func (m *MesosMessenger) sendLoop() {
	for {
		select {
		case <-m.stop:
			return
		case msg := <-m.sendingQueue:
			e := func() error {
				//TODO(jdef) implement timeout for context
				ctx, cancel := context.WithCancel(context.TODO())
				defer cancel()

				c := make(chan error, 1)
				go func() { c <- m.tr.Send(ctx, msg) }()

				select {
				case <-ctx.Done():
					// Transport layer must use the context to detect cancelled requests.
					<-c // wait for Send to return
					return ctx.Err()
				case err := <-c:
					return err
				}
			}()
			if e != nil {
				m.reportError(fmt.Errorf("Failed to send message %v: %v", msg.Name, e))
			}
		}
	}
}

// Since HTTPTransporter.Recv() is already buffered, so we don't need a 'recvLoop' here.
func (m *MesosMessenger) decodeLoop() {
	for {
		select {
		case <-m.stop:
			return
		default:
		}
		msg := m.tr.Recv()
		log.V(2).Infof("Receiving message %v from %v\n", msg.Name, msg.UPID)
		msg.ProtoMessage = reflect.New(m.installedMessages[msg.Name]).Interface().(proto.Message)
		if err := proto.Unmarshal(msg.Bytes, msg.ProtoMessage); err != nil {
			log.Errorf("Failed to unmarshal message %v: %v\n", msg, err)
			continue
		}
		// TODO(yifan): Catch panic.
		m.installedHandlers[msg.Name](msg.UPID, msg.ProtoMessage)
	}
}

// getMessageName returns the name of the message in the mesos manner.
func getMessageName(msg proto.Message) string {
	return fmt.Sprintf("%v.%v", "mesos.internal", reflect.TypeOf(msg).Elem().Name())
}
