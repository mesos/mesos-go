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
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
	"github.com/mesos/mesos-go/healthchecker"
	"github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
)

const (
	// TODO(yifan): Make them as flags.
	defaultRecoveryTimeout      = time.Minute * 15
	defaultHealthCheckDuration  = time.Second * 1
	defaultHealthCheckThreshold = 10
	// MesosVersion indicates the supported mesos version.
	MesosVersion = "0.20.0"
)

// Executor interface defines all the functions that are needed to implement
// a mesos executor.
type Executor interface {
	Registered(ExecutorDriver, *mesosproto.ExecutorInfo, *mesosproto.FrameworkInfo, *mesosproto.SlaveInfo)
	Reregistered(ExecutorDriver, *mesosproto.SlaveInfo)
	Disconnected(ExecutorDriver)
	LaunchTask(ExecutorDriver, *mesosproto.TaskInfo)
	KillTask(ExecutorDriver, *mesosproto.TaskID)
	FrameworkMessage(ExecutorDriver, string)
	Shutdown(ExecutorDriver)
	Error(ExecutorDriver, string)
}

// ExecutorDriver interface defines the functions that are needed to implement
// a mesos executor driver.
type ExecutorDriver interface {
	Start() (mesosproto.Status, error)
	Stop() mesosproto.Status
	Abort() mesosproto.Status
	Join() mesosproto.Status
	SendStatusUpdate(*mesosproto.TaskStatus) (mesosproto.Status, error)
	SendFrameworkMessage(string) (mesosproto.Status, error)
	Destroy() error
}

// MesosExecutorDriver is a implementation of the ExecutorDriver.
type MesosExecutorDriver struct {
	self               *upid.UPID
	Executor           Executor
	mutex              *sync.Mutex
	cond               *sync.Cond
	stopCh             chan struct{}
	stopped            bool
	status             mesosproto.Status
	messenger          messenger.Messenger
	slaveUPID          *upid.UPID
	slaveID            *mesosproto.SlaveID
	frameworkID        *mesosproto.FrameworkID
	executorID         *mesosproto.ExecutorID
	workDir            string
	connected          bool
	connection         uuid.UUID
	local              bool
	directory          string
	checkpoint         bool
	recoveryTimeout    time.Duration
	slaveHealthChecker healthchecker.HealthChecker
	updates            map[string]*mesosproto.StatusUpdate // Key is a UUID string.
	tasks              map[string]*mesosproto.TaskInfo     // Key is a UUID string.
}

// NewMesosExecutorDriver creates a new mesos executor driver.
func NewMesosExecutorDriver() *MesosExecutorDriver {
	driver := &MesosExecutorDriver{
		status:  mesosproto.Status_DRIVER_NOT_STARTED,
		stopped: true,
		mutex:   new(sync.Mutex),
		updates: make(map[string]*mesosproto.StatusUpdate),
		tasks:   make(map[string]*mesosproto.TaskInfo),
		workDir: ".",
	}
	driver.cond = sync.NewCond(driver.mutex)
	// TODO(yifan): Set executor cnt.
	driver.messenger = messenger.NewMesosMessenger(&upid.UPID{ID: "executor(1)"})
	if err := driver.init(); err != nil {
		log.Errorf("Failed to initialize the driver: %v\n", err)
		return nil
	}
	return driver
}

// init initializes the driver.
func (driver *MesosExecutorDriver) init() error {
	log.Infof("Init mesos executor driver\n")
	log.Infof("Version: %v\n", MesosVersion)

	// Parse environments.
	if err := driver.parseEnviroments(); err != nil {
		log.Errorf("Failed to parse environments: %v\n", err)
		return err
	}
	// Install handlers.
	driver.messenger.Install(driver.registered, &mesosproto.ExecutorRegisteredMessage{})
	driver.messenger.Install(driver.reregistered, &mesosproto.ExecutorReregisteredMessage{})
	driver.messenger.Install(driver.reconnect, &mesosproto.ReconnectExecutorMessage{})
	driver.messenger.Install(driver.runTask, &mesosproto.RunTaskMessage{})
	driver.messenger.Install(driver.killTask, &mesosproto.KillTaskMessage{})
	driver.messenger.Install(driver.statusUpdateAcknowledgement, &mesosproto.StatusUpdateAcknowledgementMessage{})
	driver.messenger.Install(driver.frameworkMessage, &mesosproto.FrameworkToExecutorMessage{})
	driver.messenger.Install(driver.shutdown, &mesosproto.ShutdownExecutorMessage{})
	driver.slaveHealthChecker = healthchecker.NewSlaveHealthChecker(driver.slaveUPID, 0, 0, 0)
	return nil
}

func (driver *MesosExecutorDriver) parseEnviroments() error {
	var value string

	value = os.Getenv("MESOS_LOCAL")
	if len(value) > 0 {
		driver.local = true
	}

	value = os.Getenv("MESOS_SLAVE_PID")
	if len(value) == 0 {
		return fmt.Errorf("Cannot find MESOS_SLAVE_PID in the environment")
	}
	upid, err := upid.Parse(value)
	if err != nil {
		log.Errorf("Cannot parse UPID %v\n", err)
		return err
	}
	driver.slaveUPID = upid

	value = os.Getenv("MESOS_SLAVE_ID")
	if len(value) == 0 {
		return fmt.Errorf("Cannot find MESOS_SLAVE_ID in the environment")
	}
	driver.slaveID = &mesosproto.SlaveID{Value: proto.String(value)}

	value = os.Getenv("MESOS_FRAMEWORK_ID")
	if len(value) == 0 {
		return fmt.Errorf("Cannot find MESOS_FRAMEWORK_ID in the environment")
	}
	driver.frameworkID = &mesosproto.FrameworkID{Value: proto.String(value)}

	value = os.Getenv("MESOS_EXECUTOR_ID")
	if len(value) == 0 {
		return fmt.Errorf("Cannot find MESOS_EXECUTOR_ID in the environment")
	}
	driver.executorID = &mesosproto.ExecutorID{Value: proto.String(value)}

	value = os.Getenv("MESOS_DIRECTORY")
	if len(value) > 0 {
		driver.workDir = value
	}

	value = os.Getenv("MESOS_CHECKPOINT")
	if value == "1" {
		driver.checkpoint = true
	}
	// TODO(yifan): Parse the duration. For now just use default.
	return nil
}

// Start starts the driver.
func (driver *MesosExecutorDriver) Start() (mesosproto.Status, error) {
	log.Infoln("Starting the executor driver")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if driver.status != mesosproto.Status_DRIVER_NOT_STARTED {
		return driver.status, nil
	}

	// Start the messenger.
	if err := driver.messenger.Start(); err != nil {
		log.Errorf("Failed to start the messenger: %v\n", err)
		return mesosproto.Status_DRIVER_NOT_STARTED, err
	}
	driver.self = driver.messenger.UPID()

	// Register with slave.
	message := &mesosproto.RegisterExecutorMessage{
		FrameworkId: driver.frameworkID,
		ExecutorId:  driver.executorID,
	}
	if err := driver.messenger.Send(driver.slaveUPID, message); err != nil {
		log.Errorf("Failed to send %v: %v\n", message, err)
		driver.messenger.Stop()
		return mesosproto.Status_DRIVER_NOT_STARTED, err
	}

	// Start monitoring the slave.
	driver.stopCh = make(chan struct{})
	driver.stopped = false
	go driver.monitorSlave()

	// Set status.
	driver.status = mesosproto.Status_DRIVER_RUNNING
	log.Infoln("Mesos executor is running")
	return driver.status, nil
}

func (driver *MesosExecutorDriver) stop(status mesosproto.Status) {
	if driver.status != mesosproto.Status_DRIVER_RUNNING {
		return
	}
	driver.messenger.Stop()
	close(driver.stopCh)
	driver.status = status
	driver.stopped = true
	driver.cond.Signal()
}

// Stop stops the driver.
func (driver *MesosExecutorDriver) Stop() mesosproto.Status {
	log.Infoln("Stopping the executor driver")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	driver.stop(mesosproto.Status_DRIVER_STOPPED)
	return driver.status
}

// Abort aborts the driver.
func (driver *MesosExecutorDriver) Abort() mesosproto.Status {
	log.Infoln("Aborting the executor driver")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	driver.stop(mesosproto.Status_DRIVER_ABORTED)
	return driver.status
}

// Join blocks the driver until it's either stopped or aborted.
func (driver *MesosExecutorDriver) Join() mesosproto.Status {
	log.Infoln("Waiting for the executor driver to stop")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if driver.status != mesosproto.Status_DRIVER_RUNNING {
		return driver.status
	}
	for driver.status == mesosproto.Status_DRIVER_RUNNING {
		driver.cond.Wait()
	}
	return driver.status
}

// SendStatusUpdate sends a StatusUpdate message to the slave.
func (driver *MesosExecutorDriver) SendStatusUpdate(taskStatus *mesosproto.TaskStatus) (mesosproto.Status, error) {
	log.Infoln("Sending status update")

	if taskStatus.GetState() == mesosproto.TaskState_TASK_STAGING {
		log.Errorf("Executor is not allowed to send TASK_STAGING status update. Aborting!\n")
		driver.Abort()
		err := fmt.Errorf("Attempted to send TASK_STAGING status update")
		driver.Executor.Error(driver, err.Error())
		return driver.status, err
	}

	// Set up status update.
	update := driver.makeStatusUpdate(taskStatus)
	log.Infof("Executor sending status update %v\n", update.String())

	// Capture the status update.
	driver.updates[uuid.UUID(update.GetUuid()).String()] = update

	// Put the status update in the message.
	message := &mesosproto.StatusUpdateMessage{
		Update: update,
		Pid:    proto.String(driver.self.String()),
	}
	// Send the message.
	if err := driver.messenger.Send(driver.slaveUPID, message); err != nil {
		log.Errorf("Failed to send %v: %v\n")
		return driver.status, err
	}
	return driver.status, nil
}

// SendFrameworkMessage sends a FrameworkMessage to the slave.
func (driver *MesosExecutorDriver) SendFrameworkMessage(data string) (mesosproto.Status, error) {
	log.Infoln("Sending framework message")

	if driver.status != mesosproto.Status_DRIVER_RUNNING {
		return driver.status, nil
	}
	message := &mesosproto.ExecutorToFrameworkMessage{
		SlaveId:     driver.slaveID,
		FrameworkId: driver.frameworkID,
		ExecutorId:  driver.executorID,
		Data:        []byte(data),
	}
	// Send the message.
	if err := driver.messenger.Send(driver.slaveUPID, message); err != nil {
		log.Errorf("Failed to send %v: %v\n")
		return driver.status, err
	}
	return driver.status, nil
}

// Destroy destroys the driver. No-op for now.
func (driver *MesosExecutorDriver) Destroy() error {
	return nil
}

func (driver *MesosExecutorDriver) registered(from *upid.UPID, pbMsg proto.Message) {
	log.Infoln("Executor driver registered")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	msg := pbMsg.(*mesosproto.ExecutorRegisteredMessage)
	slaveID := msg.GetSlaveId()
	executorInfo := msg.GetExecutorInfo()
	frameworkInfo := msg.GetFrameworkInfo()
	slaveInfo := msg.GetSlaveInfo()

	if driver.stopped {
		log.Infof("Ignoring registered message from slave %v, because the driver is stopped!\n", slaveID)
		return
	}

	log.Infof("Registered on slave %v\n", slaveID)
	driver.connected = true
	driver.connection = uuid.NewUUID()
	driver.Executor.Registered(driver, executorInfo, frameworkInfo, slaveInfo)
}

func (driver *MesosExecutorDriver) reregistered(from *upid.UPID, pbMsg proto.Message) {
	log.Infoln("Executor driver reregistered")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	msg := pbMsg.(*mesosproto.ExecutorReregisteredMessage)
	slaveID := msg.GetSlaveId()
	slaveInfo := msg.GetSlaveInfo()

	if driver.stopped {
		log.Infof("Ignoring re-registered message from slave %v, because the driver is stopped!\n", slaveID)
		return
	}

	log.Infof("Re-registered on slave %v\n", slaveID)
	driver.connected = true
	driver.connection = uuid.NewUUID()
	driver.Executor.Reregistered(driver, slaveInfo)
}

func (driver *MesosExecutorDriver) reconnect(from *upid.UPID, pbMsg proto.Message) {
	log.Infoln("Executor driver reconnect")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	msg := pbMsg.(*mesosproto.ReconnectExecutorMessage)
	slaveID := msg.GetSlaveId()

	if driver.stopped {
		log.Infof("Ignoring reconnect message from slave %v, because the driver is stopped!\n", slaveID)
		return
	}

	log.Infof("Received reconnect request from slave %v\n", slaveID)
	driver.slaveUPID = from

	message := &mesosproto.ReregisterExecutorMessage{
		ExecutorId:  driver.executorID,
		FrameworkId: driver.frameworkID,
	}
	// Send all unacknowledged updates.
	for _, u := range driver.updates {
		message.Updates = append(message.Updates, u)
	}
	// Send all unacknowledged tasks.
	for _, t := range driver.tasks {
		message.Tasks = append(message.Tasks, t)
	}
	// Send the message.
	if err := driver.messenger.Send(driver.slaveUPID, message); err != nil {
		log.Errorf("Failed to send %v: %v\n")
	}
	driver.slaveHealthChecker.Continue(driver.slaveUPID)
}

func (driver *MesosExecutorDriver) runTask(from *upid.UPID, pbMsg proto.Message) {
	log.Infoln("Executor driver runTask")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	msg := pbMsg.(*mesosproto.RunTaskMessage)
	task := msg.GetTask()
	taskID := task.GetTaskId()

	if driver.stopped {
		log.Infof("Ignoring run task message for task %v because the driver is stopped!\n", taskID)
		return
	}
	if _, ok := driver.tasks[taskID.String()]; ok {
		log.Fatalf("Unexpected duplicate task %v\n", taskID)
	}

	log.Infof("Executor asked to run task '%v'\n", taskID)
	driver.tasks[taskID.String()] = task
	driver.Executor.LaunchTask(driver, task)
}

func (driver *MesosExecutorDriver) killTask(from *upid.UPID, pbMsg proto.Message) {
	log.Infoln("Executor driver killTask")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	msg := pbMsg.(*mesosproto.KillTaskMessage)
	taskID := msg.GetTaskId()

	if driver.stopped {
		log.Infof("Ignoring kill task message for task %v, because the driver is stopped!\n", taskID)
		return
	}

	log.Infof("Executor driver is asked to kill task '%v'\n", taskID)
	driver.Executor.KillTask(driver, taskID)
}

func (driver *MesosExecutorDriver) statusUpdateAcknowledgement(from *upid.UPID, pbMsg proto.Message) {
	log.Infoln("Executor statusUpdateAcknowledgement")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	msg := pbMsg.(*mesosproto.StatusUpdateAcknowledgementMessage)
	log.Infof("Receiving status update acknowledgement %v", msg)

	frameworkID := msg.GetFrameworkId()
	taskID := msg.GetTaskId()
	uuid := uuid.UUID(msg.GetUuid())

	if driver.stopped {
		log.Infof("Ignoring status update acknowledgement %v for task %v of framework %v because the driver is stopped!\n",
			uuid, taskID, frameworkID)
	}

	// Remove the corresponding update.
	delete(driver.updates, uuid.String())
	// Remove the corresponding task.
	delete(driver.tasks, taskID.String())
}

func (driver *MesosExecutorDriver) frameworkMessage(from *upid.UPID, pbMsg proto.Message) {
	log.Infoln("Executor driver received frameworkMessage")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	msg := pbMsg.(*mesosproto.FrameworkToExecutorMessage)
	data := msg.GetData()

	if driver.stopped {
		log.Infof("Ignoring framework message because the driver is stopped!\n")
		return
	}

	log.Infof("Executor driver receives framework message\n")
	driver.Executor.FrameworkMessage(driver, string(data))
}

func (driver *MesosExecutorDriver) shutdown(from *upid.UPID, pbMsg proto.Message) {
	log.Infoln("Executor driver received shutdown")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	_, ok := pbMsg.(*mesosproto.ShutdownExecutorMessage)
	if !ok {
		panic("Not a ShutdownExecutorMessage! This should not happen")
	}

	if driver.stopped {
		log.Infof("Ignoring shutdown message because the driver is stopped!\n")
		return
	}

	log.Infof("Executor driver is asked to shutdown\n")

	if !driver.local {
		// TODO(yifan): go kill.
	}
	driver.Executor.Shutdown(driver)
	driver.stop(mesosproto.Status_DRIVER_ABORTED)
}

func (driver *MesosExecutorDriver) slaveDisconnected() {
	log.Infoln("Slave disconnected")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if driver.stopped {
		log.Infof("Ignoring slave exited event because the driver is stopped!\n")
		return
	}

	if driver.checkpoint && driver.connected {
		driver.connected = false
		log.Infof("Slave exited, but framework has checkpointing enabled. Waiting %v to reconnect with slave %v",
			driver.recoveryTimeout, driver.slaveID)
		driver.Executor.Disconnected(driver)
		time.AfterFunc(driver.recoveryTimeout, func() { driver.recoveryTimeouts(driver.connection) })
		return
	}

	log.Infof("Slave exited ... shutting down\n")
	driver.connected = false
	// Clean up
	driver.Executor.Shutdown(driver)
	driver.stop(mesosproto.Status_DRIVER_NOT_STARTED)
}

func (driver *MesosExecutorDriver) monitorSlave() {
	for {
		select {
		case <-driver.stopCh:
			driver.slaveHealthChecker.Stop()
			return
		case <-driver.slaveHealthChecker.Start():
			log.Warningf("Slave unhealthy count exceeds the threshold, assuming it is disconnected\n")
			driver.slaveHealthChecker.Pause()
			driver.slaveDisconnected()
		}
	}
}

func (driver *MesosExecutorDriver) recoveryTimeouts(connection uuid.UUID) {
	log.Infoln("Slave recovery timeouts")
	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if driver.connected {
		return
	}

	if bytes.Equal(connection, driver.connection) {
		log.Infof("Recovery timeout of %v exceeded; Shutting down\n", driver.recoveryTimeout)
		// Clean up
		driver.Executor.Shutdown(driver)
		driver.stop(mesosproto.Status_DRIVER_ABORTED)
	}
}

func (driver *MesosExecutorDriver) makeStatusUpdate(taskStatus *mesosproto.TaskStatus) *mesosproto.StatusUpdate {
	now := float64(time.Now().Unix())
	// Fill in all the fields.
	taskStatus.Timestamp = proto.Float64(now)
	taskStatus.SlaveId = driver.slaveID
	update := &mesosproto.StatusUpdate{
		FrameworkId: driver.frameworkID,
		ExecutorId:  driver.executorID,
		SlaveId:     driver.slaveID,
		Status:      taskStatus,
		Timestamp:   proto.Float64(now),
		Uuid:        uuid.NewUUID(),
	}
	return update
}
