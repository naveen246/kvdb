package service

import (
	"github.com/hashicorp/raft"
	"log"
	"net"
	"net/http"
	"strings"
)

// Store is the interface RaftHandler-backed key-value stores must implement.
type Store interface {
	// Get returns the value for the given key.
	Get(key string) string

	// Set sets the value for the given key, via distributed consensus.
	Set(key, value string) error

	// Delete removes the given key, via distributed consensus.
	Delete(key string) error

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
	addr     string
	listener net.Listener

	store       Store
	raftHandler RaftHandler
}

// New returns an uninitialized HTTP service.
func New(addr string, store Store, raftHandler RaftHandler) *Service {
	return &Service{
		addr:        addr,
		store:       store,
		raftHandler: raftHandler,
	}
}

// Start starts the service.
func (s *Service) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.listener = listener
	http.Handle("/", s)
	server := http.Server{Handler: s}

	go func() {
		err := server.Serve(s.listener)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()

	return nil
}

// Close closes the service.
func (s *Service) Close() {
	s.listener.Close()
	return
}

// ServeHTTP allows Service to serve HTTP requests.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/kv") {
		s.handleKVRequest(w, r)
	} else if r.URL.Path == "/raft" {
		s.handleRaftRequest(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// Addr returns the address on which the Service is listening
func (s *Service) Addr() net.Addr {
	return s.listener.Addr()
}
