package mesos_test

import (
	"testing"
)

func BenchmarkPrecisionScalarMath(b *testing.B) {
	var (
		start   = resources(resource(name("cpus"), valueScalar(1.001)))
		current = start.Clone()
	)
	for i := 0; i < b.N; i++ {
		current = current.Plus(current...).Plus(current...).Minus(current...).Minus(current...)
	}
}
