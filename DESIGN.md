## Overview

### Target Audience

### Developer A: high-level framework writer

This is a longer-term goal. The more immediate need is for a low-level API (see the Developer B section).

Goals:
  - wants to provide barebones initial configuration
  - wants to consume a simple event stream, or write a simple polling loop
  - wants to inject events into a managed event loop (easy state machine semantics)

One approach is an event-stream based API, where `*client.Event` objects flow through channels:

```go
func main() {
...
	c := client.New(scheduler-client-options..) // package scheduler/client
	eventCh, errCh := c.ListenWithReconnect()
eventLoop:
	for {
		select {
		case err := <-errCh:
			log.Fatalf("FATAL: unrecoverable driver error: %v", err)

		case event, ok := <-eventCh:
			if !ok {
				// graceful driver termination
				break eventLoop
			}
			processEvent(event)
		}
	}
	log.Println("scheduler terminating")
}

func processEvent(c *client.Client, e *client.Event) {
...
	if ... {
		go func() {
			// do some time-intensive task and process the result
			// on the driver event queue (easy serialization)
			c.doLater(&customEvent{...})
		}()
	}
...
	c.AcceptResources(...)
}
```

Alternate approaches:
- callback-style API
  - similar to old mesos-bindings, framework writer submits an `EventHandler`
  - scheduler driver invokes `EventHandler.Handle()` for incoming events
  - pro: a rich interface for EventHandler forces the user to consider all possible event types
  - con: the driver yields total program control to the framework-writer
  - con: new events require EventHandler API changes

- polling-style API
  - framework-writer periodically invokes `client.Event()` to pull the next incoming event from a driver-managed queue
  - pro: more minimal interface than callbacks; API will be more stable over time
  - pro: non-channel API allows scheduler the freedom to re-order the event queue until the last possible moment
  - pro: scheduler can still implement a high-level event-based state machine (just like the chan-based API)
  - con: framework writer is not "forced" to be made aware of new event types (vs. callback-style API)

### Developer B: low-level framework writer

Goals:
  - wants to manage mesos per-call details (specific codecs, headers, etc)
  - wants to manage entire registration lifecycle (total control over client state)
  - WIP in #211, for example:

```go
func main() {
...
	for {
		err := eventLoop(cli.Do(subscribe, httpcli.Close(true)))
		if err != nil {
			log.Println(err)
		} else {
			log.Println("disconnected")
		}
		log.Println("reconnecting..")
	}
...
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
		case scheduler.Event_SUBSCRIBED.Enum():
...
```

## References

* [MESOS/scheduler v1 HTTP API](https://github.com/apache/mesos/blob/master/docs/scheduler-http-api.md)
* [MESOS/executor HTTP API Design](https://docs.google.com/document/d/1dFmTrSZXCo5zj8H8SkJ4HT-V0z2YYnEZVV8Fd_-AupM/edit#heading=h.r7o3o3roqg12)
