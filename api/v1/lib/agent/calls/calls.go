package calls

import (
	"time"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/agent"
)

func GetHealth() *agent.Call { return &agent.Call{Type: agent.Call_GET_HEALTH} }

func GetFlags() *agent.Call { return &agent.Call{Type: agent.Call_GET_FLAGS} }

func GetVersion() *agent.Call { return &agent.Call{Type: agent.Call_GET_VERSION} }

func GetMetrics(d *time.Duration) (call *agent.Call) {
	call = &agent.Call{
		Type:       agent.Call_GET_METRICS,
		GetMetrics: &agent.Call_GetMetrics{},
	}
	if d != nil {
		call.GetMetrics.Timeout = &mesos.DurationInfo{
			Nanoseconds: d.Nanoseconds(),
		}
	}
	return
}

func GetLoggingLevel() *agent.Call { return &agent.Call{Type: agent.Call_GET_LOGGING_LEVEL} }

func SetLoggingLevel(level uint32, d time.Duration) *agent.Call {
	return &agent.Call{
		Type: agent.Call_SET_LOGGING_LEVEL,
		SetLoggingLevel: &agent.Call_SetLoggingLevel{
			Duration: mesos.DurationInfo{Nanoseconds: d.Nanoseconds()},
			Level:    level,
		},
	}
}

func ListFiles(path string) *agent.Call {
	return &agent.Call{
		Type: agent.Call_LIST_FILES,
		ListFiles: &agent.Call_ListFiles{
			Path: path,
		},
	}
}

func ReadFile(path string, offset uint64) *agent.Call {
	return &agent.Call{
		Type: agent.Call_READ_FILE,
		ReadFile: &agent.Call_ReadFile{
			Path:   path,
			Offset: offset,
		},
	}
}

func ReadFileWithLength(path string, offset, length uint64) (call *agent.Call) {
	call = ReadFile(path, offset)
	call.ReadFile.Length = &length
	return
}

func GetState() *agent.Call { return &agent.Call{Type: agent.Call_GET_STATE} }

func GetContainers() *agent.Call { return &agent.Call{Type: agent.Call_GET_CONTAINERS} }

func GetFrameworks() *agent.Call { return &agent.Call{Type: agent.Call_GET_FRAMEWORKS} }

func GetExecutors() *agent.Call { return &agent.Call{Type: agent.Call_GET_EXECUTORS} }

func GetTasks() *agent.Call { return &agent.Call{Type: agent.Call_GET_TASKS} }

func GetAgent() *agent.Call { return &agent.Call{Type: agent.Call_GET_AGENT} }

func LaunchNestedContainer(cid mesos.ContainerID, cmd *mesos.CommandInfo, ci *mesos.ContainerInfo) *agent.Call {
	return &agent.Call{
		Type: agent.Call_LAUNCH_NESTED_CONTAINER,
		LaunchNestedContainer: &agent.Call_LaunchNestedContainer{
			ContainerID: cid,
			Command:     cmd,
			Container:   ci,
		},
	}
}

func WaitNestedContainer(cid mesos.ContainerID) *agent.Call {
	return &agent.Call{
		Type: agent.Call_WAIT_NESTED_CONTAINER,
		WaitNestedContainer: &agent.Call_WaitNestedContainer{
			ContainerID: cid,
		},
	}
}

func KillNestedContainer(cid mesos.ContainerID) *agent.Call {
	return &agent.Call{
		Type: agent.Call_KILL_NESTED_CONTAINER,
		KillNestedContainer: &agent.Call_KillNestedContainer{
			ContainerID: cid,
		},
	}
}

func RemoveNestedContainer(cid mesos.ContainerID) *agent.Call {
	return &agent.Call{
		Type: agent.Call_REMOVE_NESTED_CONTAINER,
		RemoveNestedContainer: &agent.Call_RemoveNestedContainer{
			ContainerID: cid,
		},
	}
}

func LaunchNestedContainerSession(cid mesos.ContainerID, cmd *mesos.CommandInfo, ci *mesos.ContainerInfo) *agent.Call {
	return &agent.Call{
		Type: agent.Call_LAUNCH_NESTED_CONTAINER_SESSION,
		LaunchNestedContainerSession: &agent.Call_LaunchNestedContainerSession{
			ContainerID: cid,
			Command:     cmd,
			Container:   ci,
		},
	}
}

func AttachContainerOutput(cid mesos.ContainerID) *agent.Call {
	return &agent.Call{
		Type: agent.Call_ATTACH_CONTAINER_OUTPUT,
		AttachContainerOutput: &agent.Call_AttachContainerOutput{
			ContainerID: cid,
		},
	}
}

// AttachContainerInput returns a Call that is used to initiate attachment to a container's stdin.
// Callers should first send this Call followed by one or more AttachContainerInputXxx calls.
func AttachContainerInput(cid mesos.ContainerID) *agent.Call {
	return &agent.Call{
		Type: agent.Call_ATTACH_CONTAINER_INPUT,
		AttachContainerInput: &agent.Call_AttachContainerInput{
			Type:        agent.Call_AttachContainerInput_CONTAINER_ID,
			ContainerID: &cid,
		},
	}
}

func AttachContainerInputData(data []byte) *agent.Call {
	return &agent.Call{
		Type: agent.Call_ATTACH_CONTAINER_INPUT,
		AttachContainerInput: &agent.Call_AttachContainerInput{
			Type: agent.Call_AttachContainerInput_PROCESS_IO,
			ProcessIO: &agent.ProcessIO{
				Type: agent.ProcessIO_DATA,
				Data: &agent.ProcessIO_Data{
					Type: agent.ProcessIO_Data_STDIN,
					Data: data,
				},
			},
		},
	}
}

func AttachContainerInputTTY(t *mesos.TTYInfo) *agent.Call {
	return &agent.Call{
		Type: agent.Call_ATTACH_CONTAINER_INPUT,
		AttachContainerInput: &agent.Call_AttachContainerInput{
			Type: agent.Call_AttachContainerInput_PROCESS_IO,
			ProcessIO: &agent.ProcessIO{
				Type: agent.ProcessIO_CONTROL,
				Control: &agent.ProcessIO_Control{
					Type:    agent.ProcessIO_Control_TTY_INFO,
					TTYInfo: t,
				},
			},
		},
	}
}
