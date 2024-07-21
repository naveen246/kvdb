package store

import (
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
	"os"
	"testing"
	"time"
)

func testBoltStore(t testing.TB) *BoltStore {
	file, err := os.CreateTemp("", "bolt")
	assert.NoError(t, err)
	os.Remove(file.Name())

	// Successfully creates and returns a store
	store, err := NewBoltStore(file.Name())
	assert.NoError(t, err)

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestBoltStore_Implements(t *testing.T) {
	var store any = &BoltStore{}

	_, ok := store.(raft.StableStore)
	assert.True(t, ok, "BoltStore does not implement raft.StableStore")

	_, ok = store.(raft.LogStore)
	assert.True(t, ok, "BoltStore does not implement raft.LogStore")
}

func TestBoltOpenSameDBTwice(t *testing.T) {
	file, err := os.CreateTemp("", "bolt")
	assert.NoError(t, err)
	os.Remove(file.Name())
	defer os.Remove(file.Name())

	options := Options{
		Path: file.Name(),
		BoltOptions: &bbolt.Options{
			Timeout: 100 * time.Millisecond,
		},
	}
	store, err := New(options)
	assert.NoError(t, err)
	defer store.Close()

	// trying to open it again should timeout
	doneCh := make(chan error, 1)
	go func() {
		_, err := New(options)
		doneCh <- err
	}()
	select {
	case err := <-doneCh:
		assert.ErrorIs(t, err, bbolt.ErrTimeout)
	case <-time.After(1 * time.Second):
		t.Errorf("Gave up waiting for timeout response")
	}
}

func TestBoltOptionsReadOnly(t *testing.T) {
	file, err := os.CreateTemp("", "bolt")
	assert.NoError(t, err)
	defer os.Remove(file.Name())

	store, err := NewBoltStore(file.Name())
	assert.NoError(t, err)

	// Create the log
	log := testRaftLog(1, "log1")
	// Attempt to store the log
	err = store.StoreLog(log)
	assert.NoError(t, err)
	store.Close()

	options := Options{
		Path: file.Name(),
		BoltOptions: &bbolt.Options{
			Timeout:  100 * time.Millisecond,
			ReadOnly: true,
		},
	}
	readOnlyStore, err := New(options)
	assert.NoError(t, err)
	defer readOnlyStore.Close()

	result := new(raft.Log)
	err = readOnlyStore.GetLog(1, result)
	assert.NoError(t, err)

	// Verify the result of GetLog is the same as the log we sent to StoreLog
	assert.Equal(t, log, result)

	// Attempt to store the log, should fail on a read-only store
	err = readOnlyStore.StoreLog(log)
	assert.ErrorIs(t, err, bbolt.ErrDatabaseReadOnly)
}

func TestNewBoltStore(t *testing.T) {
	file, err := os.CreateTemp("", "bolt")
	assert.NoError(t, err)
	os.Remove(file.Name())
	defer os.Remove(file.Name())

	// Successfully creates and returns a store
	store, err := NewBoltStore(file.Name())
	assert.NoError(t, err)

	// Ensure the file was created
	assert.Equal(t, file.Name(), store.path)
	_, err = os.Stat(file.Name())
	assert.NoError(t, err)

	// Close the store so we can open again
	err = store.Close()
	assert.NoError(t, err)

	// Ensure our buckets were created
	db, err := bbolt.Open(file.Name(), fileMode, nil)
	assert.NoError(t, err)
	tx, err := db.Begin(true)
	assert.NoError(t, err)

	_, err = tx.CreateBucket(logBucket)
	assert.ErrorIs(t, err, bbolt.ErrBucketExists)
	_, err = tx.CreateBucket(storeBucket)
	assert.ErrorIs(t, err, bbolt.ErrBucketExists)
}

func TestBoltStoreIndex(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), idx)

	// Should get 0 index on empty log
	idx, err = store.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), idx)

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err = store.StoreLogs(logs)
	assert.NoError(t, err)

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), idx)

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), idx)
}

func TestBoltStoreSetAndGetLogs(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	err := store.GetLog(1, log)
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	// Create set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	err = store.StoreLogs(logs)
	assert.NoError(t, err)

	count, err := store.logCount()
	assert.NoError(t, err)
	assert.Equal(t, len(logs), count)

	// Should return the proper logs
	err = store.GetLog(1, log)
	assert.NoError(t, err)
	assert.Equal(t, logs[0], log)

	err = store.GetLog(2, log)
	assert.NoError(t, err)
	assert.Equal(t, logs[1], log)

	err = store.GetLog(3, log)
	assert.NoError(t, err)
	assert.Equal(t, logs[2], log)
}

func TestBoltStoreSetLog(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create the log
	log := testRaftLog(1, "log1")

	// Attempt to store the log
	err := store.StoreLog(log)
	assert.NoError(t, err)

	// Retrieve the log again
	result := new(raft.Log)
	err = store.GetLog(1, result)
	assert.NoError(t, err)
	assert.Equal(t, log, result)
}

func TestBoltStoreDeleteRange(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}

	// Attempt to store the logs
	err := store.StoreLogs(logs)
	assert.NoError(t, err)

	// Attempt to delete a range of logs
	err = store.DeleteRange(1, 2)
	assert.NoError(t, err)

	// Ensure the logs were deleted
	err = store.GetLog(1, new(raft.Log))
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	err = store.GetLog(2, new(raft.Log))
	assert.ErrorIs(t, err, raft.ErrLogNotFound)

	// Ensure that one log is still present
	count, err := store.logCount()
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	log := new(raft.Log)
	err = store.GetLog(3, log)
	assert.NoError(t, err)
	assert.Equal(t, logs[2], log)
}

func TestBoltStoreSetAndGet(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	_, err := store.Get([]byte("test"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	err = store.Set(k, v)
	assert.NoError(t, err)

	// Try to read it back
	val, err := store.Get(k)
	assert.NoError(t, err)
	assert.Equal(t, v, val)
}

func TestBoltStore_SetUint64_GetUint64(t *testing.T) {
	store := testBoltStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	_, err := store.GetUint64([]byte("bad"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	err = store.SetUint64(k, v)
	assert.NoError(t, err)

	// Read back the value
	val, err := store.GetUint64(k)
	assert.NoError(t, err)
	assert.Equal(t, v, val)
}
