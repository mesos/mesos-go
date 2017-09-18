package httpexec

import (
	"github.com/mesos/mesos-go/api/v1/lib/client"
	"github.com/mesos/mesos-go/api/v1/lib/executor"
)

func classifyResponse(c *executor.Call) (client.ResponseClass, error) {
	return client.ResponseClassAuto, nil // TODO(jdef) fix this, ResponseClassAuto is deprecated
}
