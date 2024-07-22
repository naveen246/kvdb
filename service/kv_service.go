package service

import (
	"log"
	"net/http"
)

func (s *Service) handleKVRequest(w http.ResponseWriter, r *http.Request) {
	log.Println(r)
}
