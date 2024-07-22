package service

import (
	"encoding/json"
	"net/http"
)

type keyVal struct {
	K string `json:"k"`
	V string `json:"v"`
}

func (s *Service) SetKey(w http.ResponseWriter, r *http.Request) {
	var kv keyVal
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&kv)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.kv.Set(kv.K, kv.V)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderJSON(w, kv)
}

func (s *Service) GetKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value := s.kv.Get(key)
	s.renderJSON(w, value)
}

func (s *Service) DeleteKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	err := s.kv.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.renderJSON(w, key)
}

func (s *Service) GetKeys(w http.ResponseWriter, r *http.Request) {
	keys := s.kv.Keys()
	s.renderJSON(w, keys)
}
