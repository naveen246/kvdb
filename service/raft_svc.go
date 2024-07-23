package service

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (s *Service) handleRaftRequest(c *gin.Context) {
	var node = struct {
		NodeID string `json:"nodeID"`
		Addr   string `json:"addr"`
	}{}
	c.BindJSON(&node)
	s.raftHandler.AddNode(node.NodeID, node.Addr)
	c.String(http.StatusOK, "Node added %s - %s", node.NodeID, node.Addr)
}
