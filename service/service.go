package service

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"net/http"
)

// KV is the interface RaftHandler-backed key-value stores must implement.
type KV interface {
	// Get returns the value for the given key.
	Get(key string) string

	// Set sets the value for the given key, via distributed consensus.
	Set(key, value string) error

	// Delete removes the given key, via distributed consensus.
	Delete(key string) error

	// Keys returns the list of keys
	Keys() []string
}

type RaftHandler interface {
	// AddNode adds the node, identified by nodeID and reachable at addr, to the cluster.
	AddNode(nodeID string, addr string) error

	Leader() raft.Server

	NodeList() ([]raft.Server, error)

	Snapshot() error
}

// Service provides HTTP service.
type Service struct {
	addr        string
	kv          KV
	raftHandler RaftHandler
}

// New returns an uninitialized HTTP service.
func New(addr string, kv KV, raftHandler RaftHandler) *Service {
	return &Service{
		addr:        addr,
		kv:          kv,
		raftHandler: raftHandler,
	}
}

// Start starts the service.
func (s *Service) Start() error {
	mux := http.NewServeMux()

	// curl -X POST localhost:11001/keys/ -d '{"k":"abc", "v":"123"}'
	mux.HandleFunc("POST /keys/", s.SetKey)

	// curl localhost:11001/keys/abc/
	mux.HandleFunc("GET /keys/{key}/", s.GetKey)

	// curl -X DELETE localhost:11001/keys/abc/
	mux.HandleFunc("DELETE /keys/{key}/", s.DeleteKey)

	// curl http://localhost:11001/keys/
	mux.HandleFunc("GET /keys/", s.GetKeys)

	mux.HandleFunc("GET /raft/", s.handleRaftRequest)

	err := http.ListenAndServe(s.addr, mux)
	if err != nil {
		return err
	}

	return nil
}

// renderJSON renders 'v' as JSON and writes it as a response into w.
func (s *Service) renderJSON(w http.ResponseWriter, v any) {
	js, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}
