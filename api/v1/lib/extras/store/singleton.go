package store

import "sync/atomic"

type (
	Getter interface {
		Get() string
	}

	GetFunc func() string

	Setter interface {
		Set(string) error
	}

	SetFunc func(string) error

	// Singleton is a thread-safe abstraction to load and store a string
	Singleton interface {
		Getter
		Setter
	}

	SingletonAdapter struct {
		GetFunc
		SetFunc
	}
)

func (f GetFunc) Get() string        { return f() }
func (f SetFunc) Set(s string) error { return f(s) }

func NewInMemorySingleton() Singleton {
	var value atomic.Value
	return &SingletonAdapter{
		func() string {
			x := value.Load()
			if x == nil {
				return ""
			}
			return x.(string)
		},
		func(s string) error {
			value.Store(s)
			return nil
		},
	}
}
