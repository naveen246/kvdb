package service

import (
	"log"
	"net/http"
)

func (s *Service) handleRaftRequest(w http.ResponseWriter, r *http.Request) {
	log.Println(r)
}
