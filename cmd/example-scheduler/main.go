package main

import (
	"flag"
	"fmt"
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
		user:       "foobar",
		name:       "example",
		url:        "http://:5050/api/v1/scheduler",
		codec:      codec{Codec: &encoding.ProtobufCodec},
		timeout:    time.Second,
		checkpoint: true,
	}

	fs := flag.NewFlagSet("example-scheduler", flag.ExitOnError)
	fs.StringVar(&cfg.user, "user", cfg.user, "Framework user to register with the Mesos master")
	fs.StringVar(&cfg.name, "name", cfg.name, "Framework name to register with the Mesos master")
	fs.Var(&cfg.codec, "codec", "Codec to encode/decode scheduler API communications [protobuf, json]")
	fs.StringVar(&cfg.url, "url", cfg.url, "Mesos scheduler API URL")
	fs.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "Mesos scheduler API connection timeout")
	fs.BoolVar(&cfg.checkpoint, "checkpoint", cfg.checkpoint, "Enable/disable framework checkpointing")
	fs.StringVar(&cfg.principal, "principal", cfg.principal, "Framework principal with which to authenticate")
	fs.StringVar(&cfg.hostname, "hostname", cfg.hostname, "Framework hostname that is advertised to the master")
	fs.Var(&cfg.labels, "label", "Framework label, may be specified multiple times")
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

	frameworkInfo := &mesos.FrameworkInfo{
		User:       cfg.user,
		Name:       cfg.name,
		Checkpoint: &cfg.checkpoint,
	}
	if cfg.principal != "" {
		frameworkInfo.Principal = &cfg.principal
	}
	if cfg.hostname != "" {
		frameworkInfo.Hostname = &cfg.hostname
	}
	if len(cfg.labels) > 0 {
		log.Println("using labels:", cfg.labels)
		frameworkInfo.Labels = &mesos.Labels{Labels: cfg.labels}
	}
	subscribe := &scheduler.Call{
		Type: scheduler.Call_SUBSCRIBE.Enum(),
		Subscribe: &scheduler.Call_Subscribe{
			FrameworkInfo: frameworkInfo,
		},
	}

	registrationTokens := backoffBucket(1*time.Second, 15*time.Second, nil)
	for {
		err := eventLoop(cli.Do(subscribe, httpcli.Close(true)))
		if err != nil {
			log.Println(err)
		} else {
			log.Println("disconnected")
		}
		<-registrationTokens
		log.Println("reconnecting..")
	}
}

func eventLoop(events encoding.Decoder, conn io.Closer, err error) error {
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	for err == nil {
		var e scheduler.Event
		if err = events.Decode(&e); err != nil {
			if err == io.EOF {
				err = nil
			}
			continue
		}

		switch e.GetType().Enum() {
		case scheduler.Event_OFFERS.Enum():
		// ...

		case scheduler.Event_ERROR.Enum():
			// it's recommended that we abort and re-try subscribing; setting
			// err here will cause the event loop to terminate and the connection
			// will be reset.
			err = fmt.Errorf("ERROR: " + e.GetError().GetMessage())

		case scheduler.Event_SUBSCRIBED.Enum():
			// noop
		default:
			// handle unknown event
		}

		log.Printf("%+v\n", e)
	}
	return err
}

type config struct {
	id         string
	user       string
	name       string
	url        string
	codec      codec
	timeout    time.Duration
	checkpoint bool
	principal  string
	hostname   string
	labels     Labels
}
