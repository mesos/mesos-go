package scheduler

import (
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/stretchr/testify/mock"
)

type MockScheduler struct {
	mock.Mock
}

func NewMockScheduler() *MockScheduler {
	return &MockScheduler{}
}

func (sched *MockScheduler) Registered(SchedulerDriver, *mesos.FrameworkID, *mesos.MasterInfo) {
	sched.Called()
}

func (sched *MockScheduler) Reregistered(SchedulerDriver, *mesos.MasterInfo) {
	sched.Called()
}

func (sched *MockScheduler) Disconnected(SchedulerDriver) {
	sched.Called()
}

func (sched *MockScheduler) ResourceOffers(SchedulerDriver, []*mesos.Offer) {
	sched.Called()
}

func (sched *MockScheduler) OfferRescinded(SchedulerDriver, *mesos.OfferID) {
	sched.Called()
}

func (sched *MockScheduler) StatusUpdate(SchedulerDriver, *mesos.TaskStatus) {
	sched.Called()
}

func (sched *MockScheduler) FrameworkMessage(SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, string) {
	sched.Called()
}

func (sched *MockScheduler) SlaveLost(SchedulerDriver, *mesos.SlaveID) {
	sched.Called()
}

func (sched *MockScheduler) ExecutorLost(SchedulerDriver, *mesos.ExecutorID, *mesos.SlaveID, int) {
	sched.Called()
}

func (sched *MockScheduler) Error(d SchedulerDriver, msg string) {
	log.Error(msg)
	sched.Called()
}

func (sched *MockScheduler) Abort() (stat mesos.Status, err error) {
	sched.Called()
	return mesos.Status_DRIVER_ABORTED, nil
}

func (sched *MockScheduler) AcceptOffers([]*mesos.OfferID, []*mesos.Offer_Operation, *mesos.Filters) (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) DeclineOffer(*mesos.OfferID, *mesos.Filters) (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_STOPPED, nil
}

func (sched *MockScheduler) Join() (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) KillTask(*mesos.TaskID) (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) ReconcileTasks([]*mesos.TaskStatus) (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) RequestResources([]*mesos.Request) (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) ReviveOffers() (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) Run() (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) Start() (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) Stop(bool) (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_STOPPED, nil
}

func (sched *MockScheduler) SendFrameworkMessage(*mesos.ExecutorID, *mesos.SlaveID, string) (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}

func (sched *MockScheduler) LaunchTasks([]*mesos.OfferID, []*mesos.TaskInfo, *mesos.Filters) (mesos.Status, error) {
	sched.Called()
	return mesos.Status_DRIVER_RUNNING, nil
}
