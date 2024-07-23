package service

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (s *Service) handleRaftRequest(c *gin.Context) {
	nodeID := c.Param("nodeID")
	addr := c.Param("addr")
	s.raftHandler.AddNode(nodeID, addr)
	c.String(http.StatusOK, "Node added")
}
