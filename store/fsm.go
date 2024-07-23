package store

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"io"
	"log"
)

type fsm Store

func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	err := json.Unmarshal(l.Data, &c)
	if err != nil {
		log.Fatalf("failed to unmarshal command: %s", err.Error())
	}

	switch c.Op {
	case CmdSet:
		return f.applySet(c.Key, c.Value)
	case CmdDelete:
		return f.applyDelete(c.Key)
	default:
		log.Fatalf("unrecognized command op: %s", c.Op)
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
	return &fsmSnapshot{store: snapStore}, nil
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
