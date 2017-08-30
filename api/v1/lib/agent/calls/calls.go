package calls

import (
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/agent"
)

func AttachContainerOutput(cid mesos.ContainerID) *agent.Call {
	return &agent.Call{
		Type: agent.Call_ATTACH_CONTAINER_OUTPUT,
		AttachContainerOutput: &agent.Call_AttachContainerOutput{
			ContainerID: cid,
		},
	}
}

// AttachContainerInput returns a Call that is used to initiate attachment to contained input.
// Callers should first send this Call followed by one or more AttachContainerInputXxx calls.
func AttachContainerInput(cid *mesos.ContainerID) *agent.Call {
	return &agent.Call{
		Type: agent.Call_ATTACH_CONTAINER_INPUT,
		AttachContainerInput: &agent.Call_AttachContainerInput{
			Type:        agent.Call_AttachContainerInput_CONTAINER_ID,
			ContainerID: cid,
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

func AttachContainerInputTTYInfo(t *mesos.TTYInfo) *agent.Call {
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
