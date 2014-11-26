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
	Scheduler     Scheduler
	MasterPid     *upid.UPID
	FrameworkInfo *mesos.FrameworkInfo

	self            *upid.UPID
	stopCh          chan struct{}
	stopped         bool
	status          mesos.Status
	messenger       messenger.Messenger
	connected       bool
	connection      uuid.UUID
	local           bool
	checkpoint      bool
	recoveryTimeout time.Duration
	cache           *schedCache
	updates         map[string]*mesos.StatusUpdate // Key is a UUID string.
	tasks           map[string]*mesos.TaskInfo     // Key is a UUID string.
}

// Create a new mesos scheduler driver with the given
// scheduler, framework info,
// master address, and credential(optional)
func NewMesosSchedulerDriver(
	sched Scheduler,
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
		stopCh:        make(chan struct{}),
		status:        mesos.Status_DRIVER_NOT_STARTED,
		stopped:       true,
		connected:     false,
		cache:         newSchedCache(),
	}

	if m, err := upid.Parse("master@" + master); err != nil {
		return nil, err
	} else {
		driver.MasterPid = m
	}

	//TODO keep scheduler counter to for proper PID.
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
	driver.messenger.Install(driver.frameworkRegistered, &mesos.FrameworkRegisteredMessage{})
	driver.messenger.Install(driver.frameworkReregistered, &mesos.FrameworkReregisteredMessage{})
	driver.messenger.Install(driver.resourcesOffered, &mesos.ResourceOffersMessage{})
	driver.messenger.Install(driver.resourceOfferRescinded, &mesos.RescindResourceOfferMessage{})
	driver.messenger.Install(driver.statusUpdated, &mesos.StatusUpdateMessage{})
	driver.messenger.Install(driver.slaveLost, &mesos.LostSlaveMessage{})
	driver.messenger.Install(driver.frameworkMessageRcvd, &mesos.ExecutorToFrameworkMessage{})
	driver.messenger.Install(driver.frameworkErrorRcvd, &mesos.FrameworkErrorMessage{})
	return nil
}

// ---------------------- Handlers for Events from Master --------------- //
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

	log.Infof("Framework registered with ID=%s\n", frameworkId.GetValue())
	driver.FrameworkInfo.Id = frameworkId // generated by master.

	driver.connected = true
	driver.connection = uuid.NewUUID()
	driver.Scheduler.Registered(driver, frameworkId, masterInfo)
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

	driver.Scheduler.Reregistered(driver, msg.GetMasterInfo())

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

	pidStrings := msg.GetPids()
	if len(pidStrings) != len(msg.Offers) {
		log.Errorln("Ignoring offers, Offer count does not match Slave PID count.")
		return
	}

	for i, offer := range msg.Offers {
		if pid, err := upid.Parse(pidStrings[i]); err == nil {
			driver.cache.putOffer(offer, pid)
			log.V(1).Infof("Cached offer %s from SlavePID %s", offer.Id.GetValue(), pid)
		} else {
			log.V(1).Infoln("Failed to parse offer PID:", pid)
		}
	}

	driver.Scheduler.ResourceOffers(driver, msg.Offers)
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

	log.V(1).Infoln("Rescinding offer ", msg.OfferId.GetValue())
	driver.cache.removeOffer(msg.OfferId)
	driver.Scheduler.OfferRescinded(driver, msg.OfferId)
}

func (driver *MesosSchedulerDriver) statusUpdated(from *upid.UPID, pbMsg proto.Message) {
	msg := pbMsg.(*mesos.StatusUpdateMessage)

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.V(1).Infoln("Ignoring StatusUpdate message, the driver is aborted!")
		return
	}

	if !driver.connected {
		log.V(1).Infoln("Ignoring StatusUpdate message, the driver is not connected!")
		return
	}

	log.V(2).Infoln("Received status update from ", from.String(), " status source:", msg.GetPid())

	driver.Scheduler.StatusUpdate(driver, msg.Update.GetStatus())

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.V(1).Infoln("Not sending StatusUpdate ACK, the driver is aborted!")
		return
	}

	// Send StatusUpdate Acknowledgement
	// Only send ACK if udpate was not from this driver
	if !from.Equal(driver.self) && msg.GetPid() != from.String() {
		ackMsg := &mesos.StatusUpdateAcknowledgementMessage{
			SlaveId:     msg.Update.SlaveId,
			FrameworkId: driver.FrameworkInfo.Id,
			TaskId:      msg.Update.Status.TaskId,
			Uuid:        msg.Update.Uuid,
		}

		log.V(2).Infoln("Sending status update ACK to ", from.String())
		if err := driver.messenger.Send(driver.MasterPid, ackMsg); err != nil {
			log.Errorf("Failed to send StatusUpdate ACK message: %v\n", err)
			return
		}
	} else {
		log.V(1).Infoln("Not sending ACK, update is not from slave:", from.String())
	}
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
	driver.cache.removeSlavePid(msg.SlaveId)

	driver.Scheduler.SlaveLost(driver, msg.SlaveId)
}

func (driver *MesosSchedulerDriver) frameworkMessageRcvd(from *upid.UPID, pbMsg proto.Message) {
	log.V(1).Infoln("Handling framework message event.")

	msg := pbMsg.(*mesos.ExecutorToFrameworkMessage)

	if driver.status == mesos.Status_DRIVER_ABORTED {
		log.V(1).Infoln("Ignoring framwork message, the driver is aborted!")
		return
	}

	log.V(1).Infoln("Received Framwork Message ", msg.String())

	driver.Scheduler.FrameworkMessage(driver, msg.ExecutorId, msg.SlaveId, string(msg.Data))
}

func (driver *MesosSchedulerDriver) frameworkErrorRcvd(from *upid.UPID, pbMsg proto.Message) {
	log.V(1).Infoln("Handling framework error event.")
	msg := pbMsg.(*mesos.FrameworkErrorMessage)
	driver.error(msg.GetMessage(), true)
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

	log.Infof("Registering with master %s [%s] ", driver.MasterPid, message)
	if err := driver.messenger.Send(driver.MasterPid, message); err != nil {
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
		if err := driver.messenger.Send(driver.MasterPid, message); err != nil {
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

func (driver *MesosSchedulerDriver) LaunchTasks(offerIds []*mesos.OfferID, tasks []*mesos.TaskInfo, filters *mesos.Filters) mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}

	// Launch tasks
	if !driver.connected {
		log.Infoln("Ignoring LaunchTasks message, disconnected from master.")
		// Send statusUpdate with status=TASK_LOST for each task.
		// See sched.cpp L#823
		for _, task := range tasks {
			driver.pushLostTask(task, "Master is disconnected")
		}
		return driver.status
	}

	okTasks := make([]*mesos.TaskInfo, 0, len(tasks))

	// Set TaskInfo.executor.framework_id, if it's missing.
	for _, task := range tasks {
		if task.Executor != nil && task.Executor.FrameworkId == nil {
			task.Executor.FrameworkId = driver.FrameworkInfo.Id
		}
		okTasks = append(okTasks, task)
	}

	for _, offerId := range offerIds {
		for _, task := range okTasks {
			// Keep only the slave PIDs where we run tasks so we can send
			// framework messages directly.
			if driver.cache.containsOffer(offerId) {
				if driver.cache.getOffer(offerId).offer.SlaveId.Equal(task.SlaveId) {
					// cache the tasked slave, for future communication
					pid := driver.cache.getOffer(offerId).slavePid
					driver.cache.putSlavePid(task.SlaveId, pid)
				} else {
					log.Warningf("Attempting to launch task %s with the wrong slaveId offer %s\n", task.TaskId.GetValue(), task.SlaveId.GetValue())
				}
			} else {
				log.Warningf("Attempting to launch task %s with unknown offer %s\n", task.TaskId.GetValue(), offerId.GetValue())
			}
		}

		driver.cache.removeOffer(offerId) // if offer
	}

	// launch tasks
	message := &mesos.LaunchTasksMessage{
		FrameworkId: driver.FrameworkInfo.Id,
		OfferIds:    offerIds,
		Tasks:       okTasks,
		Filters:     filters,
	}

	if err := driver.messenger.Send(driver.MasterPid, message); err != nil {
		for _, task := range tasks {
			driver.pushLostTask(task, "Unable to launch tasks: "+err.Error())
		}
		errMsg := fmt.Sprintf("Failed to send LaunchTask message: %v", err)
		log.Errorln(errMsg)
		driver.error(errMsg, false)
		return driver.status
	}

	return driver.status
}

func (driver *MesosSchedulerDriver) pushLostTask(taskInfo *mesos.TaskInfo, why string) {
	msg := &mesos.StatusUpdateMessage{
		Update: &mesos.StatusUpdate{
			FrameworkId: driver.FrameworkInfo.Id,
			Status: &mesos.TaskStatus{
				TaskId:  taskInfo.TaskId,
				State:   mesos.TaskState_TASK_LOST.Enum(),
				Message: proto.String(why),
			},
			SlaveId:    taskInfo.SlaveId,
			ExecutorId: taskInfo.Executor.ExecutorId,
			Timestamp:  proto.Float64(float64(time.Now().Unix())),
			Uuid:       uuid.NewRandom(),
		},
	}

	// put it on internal chanel
	// will cause handler to push to attached Scheduler
	driver.statusUpdated(driver.self, msg)
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

	if err := driver.messenger.Send(driver.MasterPid, message); err != nil {
		errMsg := fmt.Sprintf("Failed to send KillTask message: %v\n", err)
		log.Errorln(errMsg)
		driver.error(errMsg, false)
		return driver.status
	}

	return driver.status
}

func (driver *MesosSchedulerDriver) RequestResources(requests []*mesos.Request) mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}
	if !driver.connected {
		log.Infoln("Ignoring request resource message, disconnected from master.")
		return driver.status
	}

	message := &mesos.ResourceRequestMessage{
		FrameworkId: driver.FrameworkInfo.Id,
		Requests:    requests,
	}

	if err := driver.messenger.Send(driver.MasterPid, message); err != nil {
		errMsg := fmt.Sprintf("Failed to send ResourceRequest message: %v\n", err)
		log.Errorln(errMsg)
		driver.error(errMsg, false)
		return driver.status
	}

	return driver.status
}

func (driver *MesosSchedulerDriver) DeclineOffer(offerId *mesos.OfferID, filters *mesos.Filters) mesos.Status {
	return driver.LaunchTasks([]*mesos.OfferID{offerId}, []*mesos.TaskInfo{}, filters)
}

func (driver *MesosSchedulerDriver) ReviveOffers() mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}
	if !driver.connected {
		log.Infoln("Ignoring revive offers message, disconnected from master.")
		return driver.status
	}

	message := &mesos.ReviveOffersMessage{
		FrameworkId: driver.FrameworkInfo.Id,
	}
	if err := driver.messenger.Send(driver.MasterPid, message); err != nil {
		errMsg := fmt.Sprintf("Failed to send ReviveOffers message: %v\n", err)
		log.Errorln(errMsg)
		driver.error(errMsg, false)
		return driver.status
	}

	return driver.status
}

func (driver *MesosSchedulerDriver) SendFrameworkMessage(executorId *mesos.ExecutorID, slaveId *mesos.SlaveID, data []byte) mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}
	if !driver.connected {
		log.Infoln("Ignoring send framework message, disconnected from master.")
		return driver.status
	}

	message := &mesos.FrameworkToExecutorMessage{
		SlaveId:     slaveId,
		FrameworkId: driver.FrameworkInfo.Id,
		ExecutorId:  executorId,
		Data:        data,
	}
	// Use list of cached slaveIds from previous offers.
	// Send frameworkMessage directly to cached slave, otherwise to master.
	if driver.cache.containsSlavePid(slaveId) {
		slavePid := driver.cache.getSlavePid(slaveId)
		if slavePid.Equal(driver.self) {
			return driver.status
		}
		if err := driver.messenger.Send(slavePid, message); err != nil {
			errMsg := fmt.Sprintf("Failed to send framework to executor message: %v\n", err)
			log.Errorln(errMsg)
			driver.error(errMsg, false)
			return driver.status
		}
	} else {
		// slavePid not cached, send to master.
		if err := driver.messenger.Send(driver.MasterPid, message); err != nil {
			errMsg := fmt.Sprintf("Failed to send framework to executor message: %v\n", err)
			log.Errorln(errMsg)
			driver.error(errMsg, false)
			return driver.status
		}
	}

	return driver.status
}

func (driver *MesosSchedulerDriver) ReconcileTasks(statuses []*mesos.TaskStatus) mesos.Status {
	if driver.status != mesos.Status_DRIVER_RUNNING {
		return driver.status
	}
	if !driver.connected {
		log.Infoln("Ignoring send Reconcile Tasks message, disconnected from master.")
		return driver.status
	}

	message := &mesos.ReconcileTasksMessage{
		FrameworkId: driver.FrameworkInfo.Id,
		Statuses:    statuses,
	}
	if err := driver.messenger.Send(driver.MasterPid, message); err != nil {
		errMsg := fmt.Sprintf("Failed to send reconcile tasks message: %v\n", err)
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

	log.V(1).Infoln("Sending error '", err, "'")
	driver.Scheduler.Error(driver, err)
}
