package detector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type testDetector string

func (d testDetector) Detect(f MasterChanged) error { return nil }

func (d testDetector) Done() <-chan struct{} { return make(<-chan struct{}) }

func TestDetectorFactoryRegister(t *testing.T) {
	Register("bbm:", func(spec string) (Master, error) {
		return testDetector("Hello!"), nil
	})
	f, ok := plugins["bbm:"]
	assert.True(t, ok)
	assert.NotNil(t, f)
}

func TestDectorFactoryNew_EmptySpec(t *testing.T) {
	m, err := New("")
	assert.NoError(t, err)
	sa, ok := m.(*Standalone)
	assert.True(t, ok)
	assert.NotNil(t, sa)
}

// TODO figure out how to test without cyclic import
// func TestDectorFactoryNew_ZkPrefix(t *testing.T) {
// 	m, err := New("zk://127.0.0.1:5050/mesos")
// 	assert.NoError(t, err)
// 	sa, ok := m.(zoo.MasterDetector)
// 	assert.True(t, ok)
// }
