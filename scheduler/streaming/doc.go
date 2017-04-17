package streaming

// Package streaming provides an alternative interface for interacting with Mesos via an
// event stream-based scheduler API. Instead of implementing interfaces callers receive
// scheduling events via a Stream object. Schedulers that want more control over the state
// of the stream can implement their own StateFn funcs. Its expected that most scheduler
// implementations will use NewSingleUse to leverage the default state machine semantics.
