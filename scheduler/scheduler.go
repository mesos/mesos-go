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

// Abstract interface for connecting a scheduler to Mesos. This
// interface is used both to manage the scheduler's lifecycle (start
// it, stop it, or wait for it to finish) and to interact with Mesos
// (e.g., launch tasks, kill tasks, etc.). See MesosSchedulerDriver
// below for a concrete example of a SchedulerDriver.
type SchedulerDriver interface {
	// Starts the scheduler driver. This needs to be called before any
	// other driver calls are made.
	Start() mesos.Status

	// Stops the scheduler driver. If the 'failover' flag is set to
	// false then it is expected that this framework will never
	// reconnect to Mesos and all of its executors and tasks can be
	// terminated. Otherwise, all executors and tasks will remain
	// running (for some framework specific failover timeout) allowing the
	// scheduler to reconnect (possibly in the same process, or from a
	// different process, for example, on a different machine).
	Stop(failover bool) mesos.Status

	// Aborts the driver so that no more callbacks can be made to the
	// scheduler. The semantics of abort and stop have deliberately been
	// separated so that code can detect an aborted driver (i.e., via
	// the return status of SchedulerDriver::join, see below), and
	// instantiate and start another driver if desired (from within the
	// same process). Note that 'Stop()' is not automatically called
	// inside 'Abort()'.
	Abort() mesos.Status

	// Waits for the driver to be stopped or aborted, possibly
	// _blocking_ the current thread indefinitely. The return status of
	// this function can be used to determine if the driver was aborted
	// (see mesos.proto for a description of Status).
	Join() mesos.Status

	// Starts and immediately joins (i.e., blocks on) the driver.
	Run() mesos.Status

	// Requests resources from Mesos (see mesos.proto for a description
	// of Request and how, for example, to request resources
	// from specific slaves). Any resources available are offered to the
	// framework via Scheduler.ResourceOffers callback, asynchronously.
	//RequestResources(requests []*mesos.Request) mesos.Status

	// Launches the given set of tasks. Any resources remaining (i.e.,
	// not used by the tasks or their executors) will be considered
	// declined. The specified filters are applied on all unused
	// resources (see mesos.proto for a description of Filters).
	// Available resources are aggregated when mutiple offers are
	// provided. Note that all offers must belong to the same slave.
	// Invoking this function with an empty collection of tasks declines
	// offers in their entirety (see Scheduler::declineOffer).
	LaunchTasks(offerID *mesos.OfferID, tasks []*mesos.TaskInfo, filters *mesos.Filters) mesos.Status

	// Kills the specified task. Note that attempting to kill a task is
	// currently not reliable. If, for example, a scheduler fails over
	// while it was attempting to kill a task it will need to retry in
	// the future. Likewise, if unregistered / disconnected, the request
	// will be dropped (these semantics may be changed in the future).
	KillTask(taskID *mesos.TaskID) mesos.Status

	// Declines an offer in its entirety and applies the specified
	// filters on the resources (see mesos.proto for a description of
	// Filters). Note that this can be done at any time, it is not
	// necessary to do this within the Scheduler::resourceOffers
	// callback.
	//DeclineOffer(offerID *mesos.OfferID, filters *mesos.Filters) mesos.Status

	// Removes all filters previously set by the framework (via
	// LaunchTasks()). This enables the framework to receive offers from
	// those filtered slaves.
	//ReviveOffers() mesos.Status

	// Sends a message from the framework to one of its executors. These
	// messages are best effort; do not expect a framework message to be
	// retransmitted in any reliable fashion.
	//SendFrameworkMessage(executorID *mesos.ExecutorID, slaveID *mesos.SlaveID,
	//	data string) mesos.Status

	// Allows the framework to query the status for non-terminal tasks.
	// This causes the master to send back the latest task status for
	// each task in 'statuses', if possible. Tasks that are no longer
	// known will result in a TASK_LOST update. If statuses is empty,
	// then the master will send the latest status for each task
	// currently known.
	//ReconcileTasks(statuses []*mesos.TaskStatus) mesos.Status
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
	//log.Infof("Version: %v\n", MesosVersion)

	// Parse environments.
	// if err := driver.parseEnviroments(); err != nil {
	// 	log.Errorf("Failed to parse environments: %v\n", err)
	// 	return err
	// }

	// Install handlers.
	driver.messenger.Install(driver.handleFrameworkRegisteredEvent, &mesos.FrameworkRegisteredMessage{})
	driver.messenger.Install(driver.handleFrameworkReregisteredEvent, &mesos.FrameworkReregisteredMessage{})
	driver.messenger.Install(driver.handleResourceOffersEvent, &mesos.ResourceOffersMessage{})
	driver.messenger.Install(driver.handleRescindResourceOfferEvent, &mesos.RescindResourceOfferMessage{})
	// driver.messenger.Install(driver.killTask, &mesosproto.KillTaskMessage{})
	// driver.messenger.Install(driver.statusUpdateAcknowledgement, &mesosproto.StatusUpdateAcknowledgementMessage{})
	// driver.messenger.Install(driver.frameworkMessage, &mesosproto.FrameworkToExecutorMessage{})
	// driver.messenger.Install(driver.shutdown, &mesosproto.ShutdownExecutorMessage{})
	// driver.slaveHealthChecker = healthchecker.NewSlaveHealthChecker(driver.slaveUPID, 0, 0, 0)

	go driver.eventLoop()
	return nil
}

func (driver *MesosSchedulerDriver) eventLoop() {
	log.Infoln("Event Loop starting...")
	for {
		select {
		//case <-driver.destroyCh:
		//return
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
			}

			// case a := <-driver.actionCh:
			// 	switch a.acType {
			// 	// dispatch events
			// 	// case actionStartDriver:
			// 	// 	a.respCh <- driver.doStart()
			// 	// case actionJoinDriver:
			// 	// 	a.respCh <- driver.doJoin()
			// 	// case actionStopDriver:
			// 	// 	a.respCh <- driver.doStop(a.param)
			// 	}
		}
	}
}

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
	msg := pbMsg.(*mesos.FrameworkRegisteredMessage)

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
	if driver.Scheduler.Registered != nil {
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

	log.V(1).Infoln("Rescinding offer ", msg.OfferId.GetValue())
	if driver.Scheduler != nil && driver.Scheduler.OfferRescinded != nil {
		driver.Scheduler.OfferRescinded(driver, msg.OfferId)
	}
}

// Starts the scheduler driver. Blocked until either stopped or aborted.
// Returns the status of the scheduler driver.
func (driver *MesosSchedulerDriver) Start() mesos.Status {
	log.Infoln("Starting the scheduler driver...")

	if driver.status != mesos.Status_DRIVER_NOT_STARTED {
		return driver.status
	}

	// Start the messenger.
	if err := driver.messenger.Start(); err != nil {
		log.Errorf("Schduler failed to start the messenger: %v\n", err)
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
		log.Errorf("Failed to send RegisterFramework message: %v\n", err)
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

	log.Infoln("Running scheduler driver with PID=", driver.self)
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
			log.Errorf("Failed to send UnregisterFramework message: %v\n", err)
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
		okTasks = append(okTasks, task)
	}

	// launch tasks
	message := &mesos.LaunchTasksMessage{
		FrameworkId: driver.FrameworkInfo.Id,
		OfferIds:    []*mesos.OfferID{offerId},
		Tasks:       okTasks,
		Filters:     filters,
	}

	if err := driver.messenger.Send(driver.MasterUPID, message); err != nil {
		log.Errorf("Failed to send LaunchTask message: %v\n", err)
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
	}

	message := &mesos.KillTaskMessage{TaskId: taskId}

	if err := driver.messenger.Send(driver.MasterUPID, message); err != nil {
		log.Errorf("Failed to send KillTask message: %v\n", err)
		return driver.status
	}

	return driver.status
}
