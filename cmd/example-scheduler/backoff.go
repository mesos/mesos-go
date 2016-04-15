package main

import (
	"fmt"
	"time"
)

// backoffBucket returns a chan that yields a struct{}{} every so often. the wait period
// between structs is between minWait and maxWait. greedy consumers that continuously read
// from the returned bucket chan will see the wait period generally increase.
//
// Note: this func panics if minWait is a non-positive value to avoid busy-looping.
func backoffBucket(minWait, maxWait time.Duration, until <-chan struct{}) <-chan struct{} {
	if maxWait < minWait {
		maxWait, minWait = minWait, maxWait
	}
	if minWait <= 0 {
		panic(fmt.Sprintf("illegal value for minWait: %v", minWait))
	}
	tokens := make(chan struct{})
	limiter := tokens
	go func() {
		d := 0 * time.Second
		t := time.NewTimer(d)
		defer t.Stop()
		for {
			select {
			case limiter <- struct{}{}:
				d *= 2
				if d > maxWait {
					d = maxWait
				}
				limiter = nil
			case <-t.C:
				if limiter != nil {
					d /= 2
				} else {
					limiter = tokens
				}
			case <-until:
				return
			}
			// important to have non-zero minWait otherwise we busy-loop
			if d == 0 {
				d = minWait
			}
			t.Reset(d)
		}
	}()
	return tokens
}
