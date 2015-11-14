package main

import (
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/mesos/mesos-go"
	"github.com/mesos/mesos-go/encoding"
	"github.com/mesos/mesos-go/httpcli"
	"github.com/mesos/mesos-go/scheduler"
)

func main() {
	cfg := config{
		user:    "foobar",
		name:    "example",
		url:     "http://:5050/api/v1/scheduler",
		codec:   codec{Codec: &encoding.ProtobufCodec},
		timeout: time.Second,
	}

	fs := flag.NewFlagSet("example-scheduler", flag.ExitOnError)
	fs.StringVar(&cfg.user, "user", cfg.user, "Framework user to register with the Mesos master")
	fs.StringVar(&cfg.name, "name", cfg.name, "Framework name to register with the Mesos master")
	fs.Var(&cfg.codec, "codec", "Codec to encode/decode scheduler API communications [protobuf, json]")
	fs.StringVar(&cfg.url, "url", cfg.url, "Mesos scheduler API URL")
	fs.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "Mesos scheduler API connection timeout")
	fs.Parse(os.Args[1:])

	if err := run(&cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg *config) error {
	cli := httpcli.New(
		httpcli.URL(cfg.url),
		httpcli.Codec(cfg.codec.Codec),
		httpcli.Do(httpcli.With(httpcli.Timeout(cfg.timeout))),
	)

	subscribe := &scheduler.Call{
		Type: scheduler.Call_SUBSCRIBE.Enum(),
		Subscribe: &scheduler.Call_Subscribe{
			FrameworkInfo: &mesos.FrameworkInfo{
				User: cfg.user,
				Name: cfg.name,
			},
		},
	}

	for {
		events, conn, err := cli.Do(subscribe)
		for err != nil {
			var e scheduler.Event
			if err := events.Decode(&e); err != nil {
				if err == io.EOF {
					err = nil
				}
				continue
			}

			switch e.GetType().Enum() {
			case scheduler.Event_OFFERS.Enum():
			// ...
			case scheduler.Event_ERROR.Enum():
			// ...
			case scheduler.Event_SUBSCRIBED.Enum():
				//TODO(jdef) we didn't specify anything for checkpoint or failover,
				//so this might not be very effective
				frameworkID := e.GetSubscribed().GetFrameworkId()
				subscribe.FrameworkID = frameworkID
				subscribe.Subscribe.FrameworkInfo.ID = frameworkID
			default:
				// handle unknown event
			}

			log.Printf("%+v\n", e)
		}
		conn.Close()

		if err != nil {
			log.Println(err)
		}
	}
}

type config struct {
	id      string
	user    string
	name    string
	url     string
	codec   codec
	timeout time.Duration
}
