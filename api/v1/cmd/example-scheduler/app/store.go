package app

import "sync/atomic"

// IDStore is a thread-safe abstraction to load and store a stringified ID.
type IDStore interface {
	Get() string
	Set(string)
}

type IDStoreAdapter struct {
	GetFunc func() string
	SetFunc func(string)
}

func (a IDStoreAdapter) Get() string {
	if a.GetFunc != nil {
		return a.GetFunc()
	}
	return ""
}

func (a IDStoreAdapter) Set(s string) {
	if a.SetFunc != nil {
		a.SetFunc(s)
	}
}

func NewInMemoryIDStore() IDStore {
	var frameworkID atomic.Value
	return &IDStoreAdapter{
		GetFunc: func() string {
			x := frameworkID.Load()
			if x == nil {
				return ""
			}
			return x.(string)
		},
		SetFunc: func(s string) {
			frameworkID.Store(s)
		},
	}
}
