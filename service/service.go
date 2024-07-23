package service

import (
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"log"
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
func (s *Service) Start() {
	router := gin.Default()

	// curl -X POST localhost:11001/keys -d '{"abc":"122"}'
	router.POST("/keys", s.SetKey)

	// curl localhost:11001/keys
	router.GET("/keys", s.GetKeys)

	// curl localhost:11001/keys/abc
	router.GET("/keys/:key", s.GetKey)

	// curl -X DELETE localhost:11001/keys/abc
	router.DELETE("/keys/:key", s.DeleteKey)

	router.POST("/raft", s.handleRaftRequest)

	go func() {
		err := router.Run(s.addr)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()
}
