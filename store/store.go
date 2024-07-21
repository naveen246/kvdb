package store

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
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

func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv[key], nil
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

func (s *Store) Join(nodeID, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	servers := configFuture.Configuration().Servers
	for _, server := range servers {
		alreadyJoined := server.ID == raft.ServerID(nodeID) && server.Address == raft.ServerAddress(addr)
		if alreadyJoined {
			s.logger.Printf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
			return nil
		}

		belongsToCluster := server.ID == raft.ServerID(nodeID) || server.Address == raft.ServerAddress(addr)
		if belongsToCluster {
			f := s.raft.RemoveServer(server.ID, 0, 0)
			if f.Error() != nil {
				return fmt.Errorf("error removing existing node %s at %s", nodeID, addr)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	s.logger.Printf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

type fsm Store

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	err := json.Unmarshal(l.Data, &c)
	if err != nil {
		log.Panicf("failed to unmarshal command: %s", err.Error())
	}

	switch c.Op {
	case CmdSet:
		return f.applySet(c.Key, c.Value)
	case CmdDelete:
		return f.applyDelete(c.Key)
	default:
		log.Panicf("unrecognized command op: %s", c.Op)
	}

	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snapStore := make(map[string]string)
	for k, v := range f.kv {
		snapStore[k] = v
	}
	return &fsmSnapshot{snapStore}, nil
}

func (f *fsm) Restore(snapshot io.ReadCloser) error {
	snapStore := make(map[string]string)
	err := json.NewDecoder(snapshot).Decode(&snapStore)
	if err != nil {
		return err
	}

	f.kv = snapStore
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kv[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.kv, key)
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		bytes, err := json.Marshal(s.store)
		if err != nil {
			return err
		}

		_, err = sink.Write(bytes)
		if err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (s *fsmSnapshot) Release() {}
