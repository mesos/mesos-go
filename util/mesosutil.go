package util

import (
	"code.google.com/p/gogoprotobuf/proto"
	mesos "github.com/mesos/mesos-go/mesosproto"
)

func NewValueRange(begin, end uint64) *mesos.Value_Range {
	return &mesos.Value_Range{Begin: proto.Uint64(begin), End: proto.Uint64(end)}
}

func NewScalarResource(name string, val float64) *mesos.Resource {
	return &mesos.Resource{
		Name:   proto.String(name),
		Type:   mesos.Value_SCALAR.Enum(),
		Scalar: &mesos.Value_Scalar{Value: proto.Float64(val)},
	}
}

func NewRangesResource(name string, ranges []*mesos.Value_Range) *mesos.Resource {
	return &mesos.Resource{
		Name:   proto.String(name),
		Type:   mesos.Value_RANGES.Enum(),
		Ranges: &mesos.Value_Ranges{Range: ranges},
	}
}

func NewSetResource(name string, items []string) *mesos.Resource {
	return &mesos.Resource{
		Name: proto.String(name),
		Type: mesos.Value_SET.Enum(),
		Set:  &mesos.Value_Set{Item: items},
	}

}

func NewFrameworkID(id string) *mesos.FrameworkID {
	return &mesos.FrameworkID{Value: proto.String(id)}
}

func NewFrameworkInfo(user, name string, frameworkId *mesos.FrameworkID) *mesos.FrameworkInfo {
	return &mesos.FrameworkInfo{
		User: proto.String(user),
		Name: proto.String(name),
		Id:   frameworkId,
	}
}

func NewMasterInfo(id string, ip, port uint32) *mesos.MasterInfo {
	return &mesos.MasterInfo{
		Id:   proto.String(id),
		Ip:   proto.Uint32(ip),
		Port: proto.Uint32(port),
	}
}

func NewOfferID(id string) *mesos.OfferID {
	return &mesos.OfferID{Value: proto.String(id)}
}

func NewOffer(offerId *mesos.OfferID, frameworkId *mesos.FrameworkID, slaveId *mesos.SlaveID, hostname string) *mesos.Offer {
	return &mesos.Offer{
		Id:          offerId,
		FrameworkId: frameworkId,
		SlaveId:     slaveId,
		Hostname:    proto.String(hostname),
	}
}

func NewSlaveID(id string) *mesos.SlaveID {
	return &mesos.SlaveID{Value: proto.String(id)}
}

func NewTaskID(id string) *mesos.TaskID {
	return &mesos.TaskID{Value: proto.String(id)}
}

func NewTaskInfo(
	name string,
	taskId *mesos.TaskID,
	slaveId *mesos.SlaveID,
	resources []*mesos.Resource,
) *mesos.TaskInfo {
	return &mesos.TaskInfo{
		Name:      proto.String(name),
		TaskId:    taskId,
		SlaveId:   slaveId,
		Resources: resources,
	}
}

func NewTaskStatus(taskId *mesos.TaskID, state mesos.TaskState) *mesos.TaskStatus {
	return &mesos.TaskStatus{
		TaskId: taskId,
		State:  mesos.TaskState(state).Enum(),
	}
}

func NewStatusUpdate(frameworkId *mesos.FrameworkID, taskStatus *mesos.TaskStatus, timestamp float64, uuid []byte) *mesos.StatusUpdate {
	return &mesos.StatusUpdate{
		FrameworkId: frameworkId,
		Status:      taskStatus,
		Timestamp:   proto.Float64(timestamp),
		Uuid:        uuid,
	}
}

func NewCommandInfo(command string) *mesos.CommandInfo {
	return &mesos.CommandInfo{Value: proto.String(command)}
}

func NewExecutorID(id string) *mesos.ExecutorID {
	return &mesos.ExecutorID{Value: proto.String(id)}
}

func NewExecutorInfo(execId *mesos.ExecutorID, command *mesos.CommandInfo) *mesos.ExecutorInfo {
	return &mesos.ExecutorInfo{
		ExecutorId: execId,
		Command:    command,
	}
}
