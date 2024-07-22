package store

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"golang.org/x/exp/maps"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	CmdSet              = "SET"
	CmdDelete           = "DELETE"
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type Store struct {
	mu sync.Mutex

	// The key-value store for the system.
	kv map[string]string

	raft   *raft.Raft
	logger *log.Logger

	RaftDir  string
	RaftAddr string
}

func NewStore() *Store {
	return &Store{
		kv:     make(map[string]string),
		logger: log.New(os.Stderr, "store: ", log.LstdFlags),
	}
}

func (s *Store) Open(bootstrapCluster bool, localID string) error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	tcpAddr, err := net.ResolveTCPAddr("tcp", s.RaftAddr)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	boltStore, err := NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bbolt store: %s", err)
	}

	s.raft, err = raft.NewRaft(config, (*fsm)(s), boltStore, boltStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}

	if bootstrapCluster {
		server := raft.Server{
			ID:      config.LocalID,
			Address: transport.LocalAddr(),
		}
		s.raft.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{server},
		})
	}

	return nil
}

func (s *Store) Get(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv[key]
}

func (s *Store) Set(key string, value string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	cmd, err := json.Marshal(command{
		Op:    CmdSet,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

	f := s.raft.Apply(cmd, raftTimeout)
	return f.Error()
}

func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	cmd, err := json.Marshal(command{
		Op:  CmdDelete,
		Key: key,
	})
	if err != nil {
		return err
	}

	f := s.raft.Apply(cmd, raftTimeout)
	return f.Error()
}

func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return maps.Keys(s.kv)
}
