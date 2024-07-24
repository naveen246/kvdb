package store

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func Test_RaftCommands(t *testing.T) {
	s := NewStore()
	os.Mkdir(testDir, os.ModePerm)
	defer os.RemoveAll(testDir)

	assert.NotNil(t, s, "failed to create store")
	s.RaftAddr = "127.0.0.1:0"
	s.RaftDir = testDir

	err := s.Open(true, "node1")
	assert.NoError(t, err, "failed to open store")

	// Simple way to ensure there is a leader.
	time.Sleep(2 * time.Second)

	// Try to add new Node
	err = s.AddNode("node2", "127.0.0.1:1")
	assert.NoError(t, err, "new Node failed to join")

	// Verify the raft servers
	servers, err := s.NodeList()
	assert.NoError(t, err, "failed getting Node list")
	assert.Equal(t, 2, len(servers))
	assert.Equal(t, Node{NodeID: "node1", RaftAddr: "127.0.0.1:0"}, servers[0])
	assert.Equal(t, Node{NodeID: "node2", RaftAddr: "127.0.0.1:1"}, servers[1])

	// Try to add same Node added previously, add request is ignored
	err = s.AddNode("node2", "127.0.0.1:1")
	assert.NoError(t, err, "new Node failed to join")

	// Verify the raft servers
	servers, err = s.NodeList()
	assert.NoError(t, err, "failed getting Node list")
	assert.Equal(t, 2, len(servers))
	assert.Equal(t, Node{NodeID: "node1", RaftAddr: "127.0.0.1:0"}, servers[0])
	assert.Equal(t, Node{NodeID: "node2", RaftAddr: "127.0.0.1:1"}, servers[1])

	// Try to add same Node added previously with new address, earlier Node should be removed and
	// new Node should be added
	err = s.AddNode("node2", "127.0.0.1:2")
	assert.NoError(t, err, "new Node failed to join")

	// Verify the raft servers
	servers, err = s.NodeList()
	assert.NoError(t, err, "failed getting Node list")
	assert.Equal(t, 2, len(servers))
	assert.Equal(t, Node{NodeID: "node1", RaftAddr: "127.0.0.1:0"}, servers[0])
	assert.Equal(t, Node{NodeID: "node2", RaftAddr: "127.0.0.1:2"}, servers[1])

	leader := s.Leader()
	assert.Equal(t, "node1", string(leader.NodeID))
	assert.Equal(t, "127.0.0.1:0", string(leader.RaftAddr))
}
