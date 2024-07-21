package store

import (
	"encoding/binary"
	"errors"
	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
)

const fileMode = 0666

var (
	// logBucket is the name of bucket in boltDB used by raft.LogStore methods for storing raft logs
	logBucket = []byte("logBucket")
	// storeBucket is the name of bucket in boltDB used by raft.StableStore methods for storing key configurations
	storeBucket = []byte("storeBucket")

	ErrKeyNotFound = errors.New("not found")
	ErrCorrupt     = errors.New("corrupt")
)

type Options struct {
	// Path is the file path to the boltDB to use
	Path string

	BoltOptions *bbolt.Options

	// NoSync causes the database to skip fsync calls after each
	// write to the log.
	NoSync bool
}

func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

// BoltStore wraps boltdb and implements the interfaces
// raft.LogStore to store raft logs and
// raft.StableStore for key/value storage. The interfaces are defined in hashicorp/raft library
type BoltStore struct {
	db   *bbolt.DB
	path string
}

func NewBoltStore(path string) (*BoltStore, error) {
	return New(Options{Path: path})
}

func New(options Options) (*BoltStore, error) {
	db, err := bbolt.Open(options.Path, fileMode, options.BoltOptions)
	if err != nil {
		return nil, err
	}

	db.NoSync = options.NoSync

	store := &BoltStore{
		db:   db,
		path: options.Path,
	}

	if !options.readOnly() {
		err := store.initialize()
		if err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

// initialize creates logBucket and storeBucket in boltDB
func (b *BoltStore) initialize() error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.CreateBucketIfNotExists(logBucket)
	if err != nil {
		return err
	}

	_, err = tx.CreateBucketIfNotExists(storeBucket)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Close the BoltStore db
func (b *BoltStore) Close() error {
	return b.db.Close()
}

// logCount returns the number of raft logs present in logBucket of boltDB
func (b *BoltStore) logCount() (int, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return tx.Bucket(logBucket).Stats().KeyN, nil
}

// -------------Implement raft.LogStore interface-----------------------//

// FirstIndex returns the first index of raft logs written. 0 for no entries.
func (b *BoltStore) FirstIndex() (uint64, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cursor := tx.Bucket(logBucket).Cursor()
	first, _ := cursor.First()
	if first == nil {
		return 0, nil
	}

	return bytesToUint64(first), nil
}

// LastIndex returns the last index of raft logs written. 0 for no entries.
func (b *BoltStore) LastIndex() (uint64, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cursor := tx.Bucket(logBucket).Cursor()
	last, _ := cursor.Last()
	if last == nil {
		return 0, nil
	}

	return bytesToUint64(last), nil
}

// GetLog gets a log entry at a given index.
func (b *BoltStore) GetLog(idx uint64, log *raft.Log) error {
	tx, err := b.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(logBucket)
	val := bucket.Get(uint64ToBytes(idx))
	if val == nil {
		return raft.ErrLogNotFound
	}

	return convertBytesToLog(val, log)
}

// StoreLog stores a log entry.
func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (b *BoltStore) StoreLogs(logs []*raft.Log) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(logBucket)
	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val := convertLogToBytes(log)
		err := bucket.Put(key, val)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (b *BoltStore) DeleteRange(min, max uint64) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	minKey := uint64ToBytes(min)
	cursor := tx.Bucket(logBucket).Cursor()
	k, _ := cursor.Seek(minKey)
	for k != nil && bytesToUint64(k) <= max {
		err := cursor.Delete()
		if err != nil {
			return err
		}

		k, _ = cursor.Next()
	}

	return tx.Commit()
}

// -------------End raft.LogStore interface implementation-----------------------//

// -------------Implement raft.StableStore interface-----------------------//

func (b *BoltStore) Set(k, v []byte) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.Bucket(storeBucket).Put(k, v)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (b *BoltStore) Get(k []byte) ([]byte, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	val := tx.Bucket(storeBucket).Get(k)
	if val == nil {
		return nil, ErrKeyNotFound
	}

	return append([]byte(nil), val...), nil
}

func (b *BoltStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

func (b *BoltStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}

	return bytesToUint64(val), nil
}

// -------------End raft.StableStore interface implementation-----------------------//

// Sync executes fdatasync() against the database file handle.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database file to sync against the disk.
func (b *BoltStore) Sync() error {
	return b.db.Sync()
}

// convertLogToBytes converts raft.Log to bytes as follows
// first 8 bytes - log.Index
// next 8 bytes - log.Term
// next 1 byte - log.Type
// next 8 bytes - len(log.Data)
// next len(log.Data) bytes - log.Data
func convertLogToBytes(log *raft.Log) []byte {
	buf := make([]byte, 0)
	var num [8]byte

	binary.BigEndian.PutUint64(num[:], log.Index)
	buf = append(buf, num[:]...)

	binary.BigEndian.PutUint64(num[:], log.Term)
	buf = append(buf, num[:]...)

	buf = append(buf, byte(log.Type))

	binary.BigEndian.PutUint64(num[:], uint64(len(log.Data)))
	buf = append(buf, num[:]...)

	buf = append(buf, log.Data...)
	return buf
}

// convertBytesToLog converts the given bytes to raft.Log
// see convertLogToBytes doc to check how the raft.Log fields map to bytes
func convertBytesToLog(buf []byte, log *raft.Log) error {
	if len(buf) < 25 {
		return ErrCorrupt
	}

	log.Index = binary.BigEndian.Uint64(buf[0:8])
	log.Term = binary.BigEndian.Uint64(buf[8:16])
	log.Type = raft.LogType(buf[16])
	dataLen := binary.BigEndian.Uint64(buf[17:25])

	log.Data = make([]byte, dataLen)
	if len(buf[25:]) < len(log.Data) {
		return ErrCorrupt
	}
	copy(log.Data, buf[25:])

	return nil
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
