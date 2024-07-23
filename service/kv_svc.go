package service

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func (s *Service) SetKey(c *gin.Context) {
	m := map[string]string{}
	err := c.BindJSON(&m)
	if err != nil {
		return
	}

	for k, v := range m {
		err := s.kv.Set(k, v)
		if err != nil {
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
		return
	}

	c.String(http.StatusOK, key)
}

func (s *Service) GetKeys(c *gin.Context) {
	c.JSON(http.StatusOK, s.kv.Keys())
}
