package service

import (
	"net/http"
)

func (s *Service) handleRaftRequest(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("raft"))
}
