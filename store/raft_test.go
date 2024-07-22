package store

import (
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func Test_StoreJoin(t *testing.T) {
	s := NewStore()
	tmpDir, _ := os.MkdirTemp("", "store_test")
	defer os.RemoveAll(tmpDir)

	assert.NotNil(t, s, "failed to create store")
	s.RaftAddr = "127.0.0.1:0"
	s.RaftDir = tmpDir

	err := s.Open(true, "node0")
	assert.NoError(t, err, "failed to open store")

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	// Try to add new node
	err = s.Join("node1", "127.0.0.1:1")
	assert.NoError(t, err, "new node failed to join")

	// Verify the raft servers
	servers := s.raft.GetConfiguration().Configuration().Servers
	assert.Equal(t, 2, len(servers))
	assert.Equal(t, raft.Server{Suffrage: raft.Voter, ID: "node0", Address: "127.0.0.1:0"}, servers[0])
	assert.Equal(t, raft.Server{Suffrage: raft.Voter, ID: "node1", Address: "127.0.0.1:1"}, servers[1])

	// Try to add same node added previously, join request is ignored
	err = s.Join("node1", "127.0.0.1:1")
	assert.NoError(t, err, "new node failed to join")

	// Verify the raft servers
	servers = s.raft.GetConfiguration().Configuration().Servers
	assert.Equal(t, 2, len(servers))
	assert.Equal(t, raft.Server{Suffrage: raft.Voter, ID: "node0", Address: "127.0.0.1:0"}, servers[0])
	assert.Equal(t, raft.Server{Suffrage: raft.Voter, ID: "node1", Address: "127.0.0.1:1"}, servers[1])

	// Try to add same node added previously with new address, earlier node should be removed and
	// new node should be added
	err = s.Join("node1", "127.0.0.1:2")
	assert.NoError(t, err, "new node failed to join")

	// Verify the raft servers
	servers = s.raft.GetConfiguration().Configuration().Servers
	assert.Equal(t, 2, len(servers))
	assert.Equal(t, raft.Server{Suffrage: raft.Voter, ID: "node0", Address: "127.0.0.1:0"}, servers[0])
	assert.Equal(t, raft.Server{Suffrage: raft.Voter, ID: "node1", Address: "127.0.0.1:2"}, servers[1])
}
