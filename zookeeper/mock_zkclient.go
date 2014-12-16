package zookeeper

import (
	"github.com/stretchr/testify/mock"
)

// MockZkClient implementation for testing with ZkClient
type MockZkClient struct {
	Connected   bool
	WatchedNode ZkNode
	Watcher     ZkClientWatcher
	mock.Mock
}

func NewMockZkClient() *MockZkClient {
	return &MockZkClient{}
}

func (c *MockZkClient) Connect(zkuris []string, path string, watcher ZkClientWatcher) error {
	c.Connected = true
	c.Watcher = watcher
	args := c.Called()
	return args.Error(0)
}

func (c *MockZkClient) WatchChildren(path string) error {
	if c.Watcher != nil {
		c.Watcher.ChildrenChanged(c, c.WatchedNode)
	}

	args := c.Called()
	return args.Error(0)
}

func (c *MockZkClient) Disconnect() {
	c.Called()
}
