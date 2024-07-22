package store

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func Test_Snapshot(t *testing.T) {
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

	s.Set("k1", "v1")
	s.Set("k2", "v2")
	s.Set("k3", "v3")

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 3, len(s.Keys()))

	s.raft.Snapshot()
	time.Sleep(500 * time.Millisecond)

	s.Delete("k1")
	s.Delete("k2")

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 1, len(s.Keys()))

	//s.raft.Restore()
}
