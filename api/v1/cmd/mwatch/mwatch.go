package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"

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
		cli = httpmaster.NewSender(
			httpcli.New(
				httpcli.Endpoint(fmt.Sprintf(
					"http://%s/api/v1", net.JoinHostPort(*masterHost, strconv.Itoa(*masterPort)))),
			).Send,
		)
		ctx       = context.Background()
		resp, err = cli.Send(ctx, calls.NonStreaming(calls.Subscribe()))
	)
	defer func() {
		if resp != nil {
			resp.Close()
		}
	}()
	if err != nil {
		panic(err)
	}
	for {
		var e master.Event
		if err := resp.Decode(&e); err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		println(e.GetType().String())
	}
}
