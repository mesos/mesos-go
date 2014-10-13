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

package scheduler

import (
	"code.google.com/p/go-uuid/uuid"
	"code.google.com/p/gogoprotobuf/proto"
	"fmt"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/mesos/mesos-go/messenger"
	"github.com/mesos/mesos-go/upid"
	"os"
	"os/user"
	"time"
)

type eventType int
type actionType int

const (
	// master-originated event messages
	eventFrameworkRegistered eventType = iota + 100
	eventFrameworkReregistered
	eventResourceOffers
	eventRescindRessourceOffer
	eventStatusUpdate
	eventExecutorToFramework
	eventLostSlave
	eventError

	// driver-originated actions
	actionStartDriver actionType = iota + 200
	actionStopDriver
	actionAbortDriver
	actionJoinDriver
	actionRunDriver
	actionRequestResources
	actionKillTask
	actionLaunchTasks
	actionDeclineOffer
	actionReviveOffers
	actionSendFrameworkMessage
	actionReconcileTask
)

// mesosEvent event sent/received to master/from slave.
type mesosEvent struct {
	evnType eventType
	from    *upid.UPID
	msg     proto.Message
}

type response struct {
	stat mesos.Status
	err  error
}

type action struct {
	acType actionType
	param  interface{}
	respCh chan *response
}

func newMesosEvent(evnType eventType, from *upid.UPID, msg proto.Message) *mesosEvent {
	return &mesosEvent{
		evnType: evnType,
		from:    from,
		msg:     msg,
	}
}

func newAction(acType actionType, param interface{}) *action {
	return &action{
		acType: acType,
		param:  param,
		respCh: make(chan *response),
	}
}

// Concrete implementation of a SchedulerDriver that connects a
// Scheduler with a Mesos master. The MesosSchedulerDriver is
// thread-safe.
//
// Note that scheduler failover is supported in Mesos. After a
// scheduler is registered with Mesos it may failover (to a new
// process on the same machine or across multiple machines) by
// creating a new driver with the ID given to it in
// Scheduler.Registered().
//
// The driver is responsible for invoking the Scheduler callbacks as
// it communicates with the Mesos master.
//
// Note that blocking on the MesosSchedulerDriver (e.g., via
// MesosSchedulerDriver.Join) doesn't affect the scheduler callbacks
// in anyway because they are handled by a different thread.
//
// TODO(yifan): examples.
// See src/examples/test_framework.cpp for an example of using the
// MesosSchedulerDriver.
type MesosSchedulerDriver struct {
	Scheduler       *Scheduler
	MasterUPID      *upid.UPID
	FrameworkInfo   *mesos.FrameworkInfo
	self            *upid.UPID
	eventCh         chan *mesosEvent
	actionCh        chan *action
	stopCh          chan struct{}
	stopped         bool
	status          mesos.Status
	messenger       messenger.Messenger
	connected       bool
	connection      uuid.UUID
	local           bool
	checkpoint      bool
	recoveryTimeout time.Duration
	updates         map[string]*mesos.StatusUpdate // Key is a UUID string.
	tasks           map[string]*mesos.TaskInfo     // Key is a UUID string.
}

// Create a new mesos scheduler driver with the given
// scheduler, framework info,
// master address, and credential(optional)
func NewMesosSchedulerDriver(
	sched *Scheduler,
	framework *mesos.FrameworkInfo,
	master string,
	credential *mesos.Credential,
) (*MesosSchedulerDriver, error) {
	if sched == nil {
		return nil, fmt.Errorf("Scheduler callbacks required.")
	}

	if framework == nil {
		return nil, fmt.Errorf("FrameworkInfo must be provided.")
	}

	if master == "" {
		return nil, fmt.Errorf("Missing master location URL.")
	}

	// set default userid
	if framework.GetUser() == "" {
		user, err := user.Current()
		if err != nil || user == nil {
			framework.User = proto.String("")
		} else {
			framework.User = proto.String(user.Username)
		}
	}

	// set default hostname
	if framework.GetHostname() == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		framework.Hostname = proto.String(host)
	}

	driver := &MesosSchedulerDriver{
		Scheduler:     sched,
		FrameworkInfo: framework,
		eventCh:       make(chan *mesosEvent, 1024),
		actionCh:      make(chan *action, 1024),
		stopCh:        make(chan struct{}),
		status:        mesos.Status_DRIVER_NOT_STARTED,
		stopped:       true,
		connected:     false,
		updates:       make(map[string]*mesos.StatusUpdate),
		tasks:         make(map[string]*mesos.TaskInfo),
	}

	if m, err := upid.Parse("master@" + master); err != nil {
		return nil, err
	} else {
		driver.MasterUPID = m
	}

	driver.messenger = messenger.NewMesosMessenger(&upid.UPID{ID: "scheduler(1)"})
	if err := driver.init(); err != nil {
		log.Errorf("Failed to initialize the scheduler driver: %v\n", err)
		return nil, err
	}
	return driver, nil
}

// init initializes the driver.
func (driver *MesosSchedulerDriver) init() error {
	log.Infof("Initializing mesos scheduler driver\n")

	// Install handlers.
	driver.messenger.Install(driver.handleFrameworkRegisteredEvent, &mesos.FrameworkRegisteredMessage{})
	driver.messenger.Install(driver.handleFrameworkReregisteredEvent, &mesos.FrameworkReregisteredMessage{})
	driver.messenger.Install(driver.handleResourceOffersEvent, &mesos.ResourceOffersMessage{})
	driver.messenger.Install(driver.handleRescindResourceOfferEvent, &mesos.RescindResourceOfferMessage{})
	driver.messenger.Install(driver.handleStatusUpdateEvent, &mesos.StatusUpdateMessage{})
	driver.messenger.Install(driver.handleLostSlaveEvent, &mesos.LostSlaveMessage{})
	driver.messenger.Install(driver.handleFrameworkMessageEvent, &mesos.ExecutorToFrameworkMessage{})

	go driver.eventLoop()
	return nil
}

func (driver *MesosSchedulerDriver) eventLoop() {
	log.Infoln("Event Loop starting...")
	for {
		select {
		case e := <-driver.eventCh:
			switch e.evnType {
			case eventFrameworkRegistered:
				driver.frameworkRegistered(e.from, e.msg)
			case eventFrameworkReregistered:
				driver.frameworkReregistered(e.from, e.msg)
			case eventResourceOffers:
				driver.resourcesOffered(e.from, e.msg)
			case eventRescindRessourceOffer:
				driver.resourceOfferRescinded(e.from, e.msg)
			case eventStatusUpdate:
				driver.statusUpdated(e.from, e.msg)
			case eventLostSlave:
				driver.slaveLost(e.from, e.msg)
			case eventExecutorToFramework:
				driver.frameworkMessageRcvd(e.from, e.msg)
			}
		}
	}
}

// ---------------------- Handlers for Events from Master --------------- //
func (driver *MesosSchedulerDriver) handleFrameworkRegisteredEvent(from *upid.UPID, msg proto.Message) {
	driver.eventCh <- newMesosEvent(eventFrameworkRegistered, from, msg)
}

func (driver *MesosSchedulerDriver) frameworkRegistered(from *upid.UPID, pbMsg proto.Message) {
	log.V(2).Infoln("Handling scheduler driver framework registered event.")

	msg := pbMsg.(*mesos.FrameworkRegisteredMessage)
	masterInfo := msg.GetMasterInfo()
	masterPid := msg.GetMasterInfo().GetPid()
	frameworkId := msg.GetFrameworkId()

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.Infof("Ignoring FrameworkRegisteredMessage from master %s, driver is aborted!\n", masterPid)
		return
	}

	if driver.connected {
		log.Infoln("Ignoring FrameworkRegisteredMessage from master %s, driver is already connected!\n", masterPid)
		return
	}

	if driver.stopped {
		log.Infof("Ignoring FrameworkRegisteredMessage from master %s, driver is stopped!\n", masterPid)
		return
	}

	log.Infof("Registered with master %s\n", masterPid)
	driver.connected = true
	driver.connection = uuid.NewUUID()
	if driver.Scheduler.Registered != nil {
		driver.Scheduler.Registered(driver, frameworkId, masterInfo)
	}
}

func (driver *MesosSchedulerDriver) handleFrameworkReregisteredEvent(from *upid.UPID, msg proto.Message) {
	driver.eventCh <- newMesosEvent(eventFrameworkReregistered, from, msg)
}

func (driver *MesosSchedulerDriver) frameworkReregistered(from *upid.UPID, pbMsg proto.Message) {
	log.V(1).Infoln("Handling Scheduler re-registered event.")
	msg := pbMsg.(*mesos.FrameworkReregisteredMessage)

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.Infoln("Ignoring FrameworkReregisteredMessage from master, driver is aborted!")
		return
	}

	if driver.connected {
		log.Infoln("Ignoring FrameworkReregisteredMessage from master,driver is already connected!")
		return
	}

	// TODO(vv) detect if message was from leading-master (sched.cpp)
	log.Infof("Framework re-registered with ID [%s] ", msg.GetFrameworkId().GetValue())
	driver.connected = true
	driver.connection = uuid.NewUUID()
	if driver.Scheduler.Reregistered != nil {
		driver.Scheduler.Reregistered(driver, msg.GetMasterInfo())
	}
}

func (driver *MesosSchedulerDriver) handleResourceOffersEvent(from *upid.UPID, msg proto.Message) {
	driver.eventCh <- newMesosEvent(eventResourceOffers, from, msg)
}

func (driver *MesosSchedulerDriver) resourcesOffered(from *upid.UPID, pbMsg proto.Message) {
	log.V(1).Infoln("Handling resource offers.")

	msg := pbMsg.(*mesos.ResourceOffersMessage)
	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.Infoln("Ignoring ResourceOffersMessage, the driver is aborted!")
		return
	}

	if !driver.connected {
		log.Infoln("Ignoring ResourceOffersMessage, the driver is not connected!")
		return
	}

	if driver.Scheduler != nil && driver.Scheduler.ResourceOffers != nil {
		driver.Scheduler.ResourceOffers(driver, msg.Offers)
	}
}

func (driver *MesosSchedulerDriver) handleRescindResourceOfferEvent(from *upid.UPID, msg proto.Message) {
	driver.eventCh <- newMesosEvent(eventRescindRessourceOffer, from, msg)
}

func (driver *MesosSchedulerDriver) resourceOfferRescinded(from *upid.UPID, pbMsg proto.Message) {
	log.V(1).Infoln("Handling resource offer rescinded.")

	msg := pbMsg.(*mesos.RescindResourceOfferMessage)

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.Infoln("Ignoring RescindResourceOfferMessage, the driver is aborted!")
		return
	}

	if !driver.connected {
		log.Infoln("Ignoring ResourceOffersMessage, the driver is not connected!")
		return
	}

	// TODO(vv) check for leading master (see sched.cpp)
	// TODO(vv) remove saved offers (see sched.cpp L#572)

	log.V(1).Infoln("Rescinding offer ", msg.OfferId.GetValue())
	if driver.Scheduler != nil && driver.Scheduler.OfferRescinded != nil {
		driver.Scheduler.OfferRescinded(driver, msg.OfferId)
	}
}

func (driver *MesosSchedulerDriver) handleStatusUpdateEvent(from *upid.UPID, msg proto.Message) {
	driver.eventCh <- newMesosEvent(eventStatusUpdate, from, msg)
}

func (driver *MesosSchedulerDriver) statusUpdated(from *upid.UPID, pbMsg proto.Message) {
	log.V(1).Infoln("Handling status update.")

	msg := pbMsg.(*mesos.StatusUpdateMessage)

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.V(1).Infoln("Ignoring StatusUpdate message, the driver is aborted!")
		return
	}

	if !driver.connected {
		log.V(1).Infoln("Ignoring StatusUpdate message, the driver is not connected!")
		return
	}

	log.V(2).Infoln("Received status update from ", from.String())

	if driver.Scheduler != nil && driver.Scheduler.StatusUpdate != nil {
		driver.Scheduler.StatusUpdate(driver, msg.Update.GetStatus())
	}

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.V(1).Infoln("Not sending StatusUpdate ACK, the driver is aborted!")
		return
	}

	// Send StatusUpdate Acknowledgement
	ackMsg := &mesos.StatusUpdateAcknowledgementMessage{
		SlaveId:     msg.Update.SlaveId,
		FrameworkId: driver.FrameworkInfo.Id,
		TaskId:      msg.Update.Status.TaskId,
		Uuid:        msg.Update.Uuid,
	}

	log.V(2).Infoln("Sending status update ACK to ", from.String())
	if err := driver.messenger.Send(driver.MasterUPID, ackMsg); err != nil {
		log.Errorf("Failed to send StatusUpdate ACK message: %v\n", err)
		return
	}
}

func (driver *MesosSchedulerDriver) handleLostSlaveEvent(from *upid.UPID, msg proto.Message) {
	driver.eventCh <- newMesosEvent(eventLostSlave, from, msg)
}

func (driver *MesosSchedulerDriver) slaveLost(from *upid.UPID, pbMsg proto.Message) {
	log.V(1).Infoln("Handling LostSlave event.")

	msg := pbMsg.(*mesos.LostSlaveMessage)

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.V(1).Infoln("Ignoring LostSlave message, the driver is aborted!")
		return
	}

	if !driver.connected {
		log.V(1).Infoln("Ignoring LostSlave message, the driver is not connected!")
		return
	}

	// TODO(VV) - detect leading master (see sched.cpp)

	log.V(2).Infoln("Lost slave ", msg.SlaveId.GetValue())

	if driver.Scheduler != nil && driver.Scheduler.SlaveLost != nil {
		driver.Scheduler.SlaveLost(driver, msg.SlaveId)
	}
}

func (driver *MesosSchedulerDriver) handleFrameworkMessageEvent(from *upid.UPID, msg proto.Message) {
	driver.eventCh <- newMesosEvent(eventExecutorToFramework, from, msg)
}

func (driver *MesosSchedulerDriver) frameworkMessageRcvd(from *upid.UPID, pbMsg proto.Message) {
	log.V(1).Infoln("Handling framework message event.")

	msg := pbMsg.(*mesos.ExecutorToFrameworkMessage)

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.V(1).Infoln("Ignoring framwork message, the driver is aborted!")
		return
	}

	log.V(1).Infoln("Received Framwork Message ", msg.String())

	if driver.Scheduler != nil && driver.Scheduler.FrameworkMessage != nil {
		driver.Scheduler.FrameworkMessage(driver, msg.ExecutorId, msg.SlaveId, msg.Data)
	}
}

// ---------------------- Interface Methods ---------------------- //

// Starts the scheduler driver. Blocked until either stopped or aborted.
// Returns the status of the scheduler driver.
func (driver *MesosSchedulerDriver) Start() mesos.Status {
	log.Infoln("Starting the scheduler driver...")

	if driver.status != mesos.Status_DRIVER_NOT_STARTED {
		return driver.status
	}

	// Start the messenger.
	if err := driver.messenger.Start(); err != nil {
		errMsg := fmt.Sprintf("Schduler failed to start the messenger: %v", err)
		log.Errorln(errMsg)
		driver.error(errMsg, false)
		driver.status = mesos.Status_DRIVER_NOT_STARTED
		return driver.status
	}
	driver.self = driver.messenger.UPID()

	// register framework
	message := &mesos.RegisterFrameworkMessage{
		Framework: driver.FrameworkInfo,
	}

	log.Infof("Registering with master %s [%s] ", driver.MasterUPID, message)
	if err := driver.messenger.Send(driver.MasterUPID, message); err != nil {
		errMsg := fmt.Sprintf("Stopping driver. Failed to send RegisterFramework message: %v", err)
		log.Errorf(errMsg)
		driver.error(errMsg, false)
		driver.messenger.Stop()
		return mesos.Status_DRIVER_NOT_STARTED
	}

	driver.status = mesos.Status_DRIVER_RUNNING
	driver.stopped = false

	log.Infoln("Mesos scheduler driver started OK.")

	// TODO(VV) Monitor Master Connection
	// go driver.monitorMaster()

	return driver.status
}

//Join blocks until the driver is stopped.
//Should follow a call to Start()
func (driver *MesosSchedulerDriver) Join() mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}
	<-driver.stopCh // wait for stop signal
	return driver.status
}

//Run starts and joins driver process and waits to be stopped or aborted.
func (driver *MesosSchedulerDriver) Run() mesos.Status {
	stat := driver.Start()
	if stat != mesos.Status_DRIVER_RUNNING {
		log.Errorln("Mesos scheduler driver failed to start. Exiting Run.")
		return stat
	}

	log.Infof("Running scheduler driver with PID=%v\n", driver.self)
	return driver.Join()
}

//Stop stops the driver.
func (driver *MesosSchedulerDriver) Stop(failover bool) mesos.Status {
	log.Infoln("Stopping the scheduler driver")
	if driver.status != mesos.Status_DRIVER_RUNNING {
		log.Error("Unexpected status ", driver.status)
		return driver.status
	}

	if driver.connected && failover {
		// unregister the framework
		message := &mesos.UnregisterFrameworkMessage{
			FrameworkId: driver.FrameworkInfo.Id,
		}
		if err := driver.messenger.Send(driver.MasterUPID, message); err != nil {
			errMsg := fmt.Sprintf("Failed to send UnregisterFramework message while stopping driver: %v", err)
			log.Errorln(errMsg)
			driver.error(errMsg, false)
			driver.messenger.Stop()
			driver.status = mesos.Status_DRIVER_ABORTED
			return driver.status
		}
	}

	// stop messenger
	driver.messenger.Stop()
	driver.status = mesos.Status_DRIVER_STOPPED
	driver.stopped = true
	driver.connected = false
	close(driver.stopCh)

	return driver.status
}

func (driver *MesosSchedulerDriver) Abort() mesos.Status {
	log.Infof("Aborting framework [%s]\n", driver.FrameworkInfo.GetId().GetValue())
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}

	if !driver.connected {
		log.Infoln("Not sending deactivate message, master is disconnected.")
		return driver.status
	}
	driver.Stop(true)
	driver.status = mesos.Status_DRIVER_ABORTED
	return driver.status
}

func (driver *MesosSchedulerDriver) LaunchTasks(offerId *mesos.OfferID, tasks []*mesos.TaskInfo, filters *mesos.Filters) mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}

	// Launch tasks
	if !driver.connected {
		log.Infoln("Ignoring LaunchTasks message, disconnected from master.")
		// TODO: send statusUpdate with status=TASK_LOST for each task.
		//       See sched.cpp L#823
		return driver.status
	}
	// only allow tasks with either ExecInfo or CommandInfo through.
	okTasks := make([]*mesos.TaskInfo, 0, len(tasks))
	for _, task := range tasks {
		if task.Executor != nil && task.Command != nil {
			log.Warning("WARN: Ignoring task ", task.Name, ". It has both Executor and Command set.")
			continue
		}
		if task.Executor != nil && task.Executor.FrameworkId != driver.FrameworkInfo.Id {
			log.Warning("WARN: Ignoring task ", task.Name, ". Expecting FrameworkId", driver.FrameworkInfo.GetId(), ", but got", task.Executor.FrameworkId.GetValue())
			continue
		}
		// ensure default frameworkid value
		if task.Executor != nil && task.Executor.FrameworkId == nil {
			task.Executor.FrameworkId = driver.FrameworkInfo.Id
		}

		// TODO (VV): Send StatusUpdate(TASK_LOST)
		//   when task.executorinfo.framework_id != driver.FrameworkInfo.Id
		//   See Sched.cpp L#867

		okTasks = append(okTasks, task)
	}

	// TODO (VV): Keep track of slaves where tasks are running so future
	//   SendFrameworkMessage can be sent directly to running slaves.
	//   See L#901

	// launch tasks
	message := &mesos.LaunchTasksMessage{
		FrameworkId: driver.FrameworkInfo.Id,
		OfferIds:    []*mesos.OfferID{offerId},
		Tasks:       okTasks,
		Filters:     filters,
	}

	if err := driver.messenger.Send(driver.MasterUPID, message); err != nil {
		errMsg := fmt.Sprintf("Failed to send LaunchTask message: %v", err)
		log.Errorln(errMsg)
		driver.error(errMsg, false)
		// TODO(VV): Task probably should be marked as lost or requeued.
		return driver.status
	}

	return driver.status
}

func (driver *MesosSchedulerDriver) KillTask(taskId *mesos.TaskID) mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}

	if !driver.connected {
		log.Infoln("Ignoring kill task message, disconnected from master.")
		return driver.status
	}

	message := &mesos.KillTaskMessage{TaskId: taskId}

	if err := driver.messenger.Send(driver.MasterUPID, message); err != nil {
		errMsg := fmt.Sprintf("Failed to send KillTask message: %v\n", err)
		log.Errorln(errMsg)
		driver.error(errMsg, false)
		return driver.status
	}

	return driver.status
}

func (driver *MesosSchedulerDriver) error(err string, abortDriver bool) {
	if abortDriver {
		if driver.status == mesos.Status_DRIVER_ABORTED {
			log.V(1).Infoln("Ignoring error message, the driver is aborted!")
			return
		}

		log.Infoln("Aborting driver, got error '", err, "'")

		driver.Abort()
	}

	if driver.Scheduler != nil && driver.Scheduler.Error != nil {
		log.V(1).Infoln("Sending error '", err, "'")
		go driver.Scheduler.Error(driver, err)
	}
}
