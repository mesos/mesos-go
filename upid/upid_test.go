package upid

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/mesosphere/testify/assert"
)

func generateRandomString() string {
	b := make([]byte, rand.Intn(1024))
	for i := range b {
		b[i] = byte(rand.Int())
	}
	return strings.Replace(string(b), "@", "", -1)
}

func TestUPID(t *testing.T) {
	u, err := Parse("mesos@localhost:5050")
	assert.NotNil(t, u)
	assert.NoError(t, err)
	assert.Equal(t, "mesos@localhost:5050", u.String())

	u, err = Parse("mesos@foo:bar")
	assert.Nil(t, u)
	assert.Error(t, err)

	u, err = Parse("mesoslocalhost5050")
	assert.Nil(t, u)
	assert.Error(t, err)

	u, err = Parse("mesos@localhost")
	assert.Nil(t, u)
	assert.Error(t, err)

	u1, err := Parse("mesos@localhost:5050")
	u2, err := Parse("mesos@localhost:5050")
	u3, err := Parse("mesos1@localhost:5050")
	u4, err := Parse("mesos@mesos.com:5050")
	u5, err := Parse("mesos@localhost:5051")

	assert.True(t, u1.Equal(u2))
	assert.False(t, u1.Equal(u3))
	assert.False(t, u1.Equal(u4))
	assert.False(t, u1.Equal(u5))

	// Simple fuzzy test.
	for i := 0; i < 100000; i++ {
		ra := generateRandomString()
		u, err = Parse(ra)
		if u != nil {
			println(ra)
		}
		assert.Nil(t, u)
		assert.Error(t, err)
	}

}
