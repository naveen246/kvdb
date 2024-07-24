package service

import (
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"log"
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

	// curl -X POST localhost:11001/raft/join -d '{ "addr": "localhost:12002", "nodeID": "node2" }'
	router.POST("/raft/join", s.RaftJoin)

	// curl localhost:11001/raft/leader
	router.GET("/raft/leader", s.RaftLeader)

	// curl localhost:11001/raft/servers
	router.GET("/raft/servers", s.RaftServers)

	go func() {
		err := router.Run(s.addr)
		if err != nil {
			log.Fatalf("HTTP serve: %s", err)
		}
	}()
}

// ************** KV Service *********************************//

func (s *Service) SetKey(c *gin.Context) {
	m := map[string]string{}
	err := c.BindJSON(&m)
	if err != nil {
		c.JSON(http.StatusBadRequest, err.Error())
		return
	}

	for k, v := range m {
		err := s.kv.Set(k, v)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err.Error())
			return
		}
	}

	c.JSON(http.StatusCreated, m)
}

func (s *Service) GetKey(c *gin.Context) {
	key := c.Param("key")
	value := s.kv.Get(key)
	c.JSON(http.StatusOK, gin.H{key: value})
}

func (s *Service) DeleteKey(c *gin.Context) {
	key := c.Param("key")
	err := s.kv.Delete(key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
	}

	c.String(http.StatusOK, key)
}

func (s *Service) GetKeys(c *gin.Context) {
	c.JSON(http.StatusOK, s.kv.Keys())
}

// ************************ Raft service *************************//

func (s *Service) RaftJoin(c *gin.Context) {
	var node = struct {
		NodeID string `json:"nodeID"`
		Addr   string `json:"addr"`
	}{}
	c.BindJSON(&node)
	s.raftHandler.AddNode(node.NodeID, node.Addr)
	c.String(http.StatusOK, "Node added %s - %s", node.NodeID, node.Addr)
}

func (s *Service) RaftLeader(c *gin.Context) {
	leader := s.raftHandler.Leader()
	c.JSON(http.StatusOK, leader)
}

func (s *Service) RaftServers(c *gin.Context) {
	servers, err := s.raftHandler.NodeList()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
	}
	c.JSON(http.StatusOK, servers)
}
