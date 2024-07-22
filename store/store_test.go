package store

import (
	"github.com/stretchr/testify/assert"
	"os"
	"slices"
	"testing"
	"time"
)

// Test_StoreOpen tests that the store can be opened.
func Test_StoreOpen(t *testing.T) {
	s := NewStore()
	tmpDir, _ := os.MkdirTemp("", "store_test")
	defer os.RemoveAll(tmpDir)

	assert.NotNil(t, s, "failed to create store")
	s.RaftAddr = "127.0.0.1:0"
	s.RaftDir = tmpDir

	err := s.Open(false, "node0")
	assert.NoError(t, err, "failed to open store")
}

// Test_StoreOpenSingleNode tests that a command can be applied to the log
func Test_StoreOpenSingleNode(t *testing.T) {
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

	err = s.Set("foo", "bar")
	assert.NoError(t, err, "failed to set key")

	err = s.Set("far", "baz")
	assert.NoError(t, err, "failed to set key")

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)

	keys := s.Keys()
	assert.Equal(t, 2, len(keys))
	assert.True(t, slices.Contains(keys, "foo"))
	assert.True(t, slices.Contains(keys, "far"))

	value := s.Get("foo")
	assert.Equal(t, "bar", value, "key has wrong value")

	err = s.Delete("foo")
	assert.NoError(t, err, "failed to delete key")

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value = s.Get("foo")
	assert.Empty(t, value, "key has wrong value")
}
