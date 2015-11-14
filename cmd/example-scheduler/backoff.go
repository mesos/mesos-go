package main

import (
	"time"
)

func backoffBucket(minWait, maxWait time.Duration, until <-chan struct{}) <-chan struct{} {
	tokens := make(chan struct{})
	limiter := tokens
	go func() {
		d := 0 * time.Second
		t := time.NewTimer(d)
		defer t.Stop()
		for {
			select {
			case limiter <- struct{}{}:
				if d == 0 {
					d = minWait
				} else {
					d *= 2
					if d > maxWait {
						d = maxWait
					}
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
			t.Reset(d)
		}
	}()
	return tokens
}
