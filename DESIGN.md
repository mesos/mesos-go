## Overview

### Target Audience

- Developer A: high-level framework writer
  - wants to provide barebones initial configuration
  - wants to consume a simple event stream
  - wants to inject events into a managed event loop (easy state machine semantics)

```go
c := client.New(connectionOptions..) // package scheduler/client
eventCh, errCh := c.Register(registrationOptions..)
for {
  // .. consume events until errCh closes; indicates disconnection from master,
  // or inject some custom event object:
  c.Inject(someCustomEvent)
}
```

- Developer B: low-level framework writer
  - wants to manage mesos per-call details (specific codecs, headers, etc)
  - wants to manage entire registration lifecycle (total control over client state)

## References

* [MESOS/scheduler HTTP API Design](https://docs.google.com/document/d/1pnIY_HckimKNvpqhKRhbc9eSItWNFT-priXh_urR-T0/edit)
* [MESOS/executor HTTP API Design](https://docs.google.com/document/d/1dFmTrSZXCo5zj8H8SkJ4HT-V0z2YYnEZVV8Fd_-AupM/edit#heading=h.r7o3o3roqg12)
