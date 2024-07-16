package store

import (
	"encoding/binary"
	"errors"
	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
)

const fileMode = 0666

var (
	logBucket   = []byte("logBucket")
	storeBucket = []byte("storeBucket")

	ErrKeyNotFound = errors.New("key not found")
	ErrCorrupt     = errors.New("corrupt")
)

type Options struct {
	Path        string
	BoltOptions *bbolt.Options
	NoSync      bool
}

func (o *Options) readOnly() bool {
	return o != nil && o.BoltOptions != nil && o.BoltOptions.ReadOnly
}

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

func (b *BoltStore) Close() error {
	return b.db.Close()
}

func (b *BoltStore) logCount() (int, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return tx.Bucket(logBucket).Stats().KeyN, nil
}

// -------------Implement raft.LogStore interface-----------------------//

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

func (b *BoltStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

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

//--------------------------------------------------------------------------//

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

//--------------------------------------------------------------------------//

func (b *BoltStore) Sync() error {
	return b.db.Sync()
}

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

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
