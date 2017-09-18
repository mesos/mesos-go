package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli"
	"github.com/mesos/mesos-go/api/v1/lib/httpcli/httpmaster"
	"github.com/mesos/mesos-go/api/v1/lib/master"
	"github.com/mesos/mesos-go/api/v1/lib/master/calls"
)

var (
	masterHost = flag.String("master", "127.0.0.1", "IP address of mesos master")
	masterPort = flag.Int("port", 5050, "Port of mesos master")
)

func init() {
	flag.Parse()
}

func main() {
	var (
		uri = fmt.Sprintf("http://%s/api/v1", net.JoinHostPort(*masterHost, strconv.Itoa(*masterPort)))
		cli = httpmaster.NewSender(httpcli.New(httpcli.Endpoint(uri)).Send)
		ctx = context.Background()
		err = watch(cli.Send(ctx, calls.NonStreaming(calls.Subscribe())))
	)
	if err != nil {
		panic(err)
	}
}

func watch(resp mesos.Response, err error) error {
	defer func() {
		if resp != nil {
			resp.Close()
		}
	}()
	if err != nil {
		return err
	}
	for {
		var e master.Event
		if err := resp.Decode(&e); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		switch t := e.GetType(); t {
		case master.Event_TASK_ADDED:
			fmt.Println(t.String(), e.GetTaskAdded().String())
		case master.Event_TASK_UPDATED:
			fmt.Println(t.String(), e.GetTaskUpdated().String())
		case master.Event_AGENT_ADDED:
			fmt.Println(t.String(), e.GetAgentAdded().String())
		case master.Event_AGENT_REMOVED:
			fmt.Println(t.String(), e.GetAgentRemoved().String())
		case master.Event_FRAMEWORK_ADDED:
			fmt.Println(t.String(), e.GetFrameworkAdded().String())
		case master.Event_FRAMEWORK_UPDATED:
			fmt.Println(t.String(), e.GetFrameworkUpdated().String())
		case master.Event_FRAMEWORK_REMOVED:
			fmt.Println(t.String(), e.GetFrameworkRemoved().String())
		default:
			fmt.Println(t.String())
		}
	}
	return nil
}
