package service

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
	"slices"
	"testing"
)

const (
	DefaultHTTPAddr = "localhost:11001"
	DefaultRaftAddr = "localhost:12001"
)

// Test_NewServer tests that a server can perform all basic operations.
func Test_NewServer(t *testing.T) {
	stor := newTestStore()
	url := fmt.Sprintf("http://%s", DefaultHTTPAddr)

	New(DefaultHTTPAddr, stor, nil).Start()

	resp := getKey(t, url, "k1")
	assert.Equal(t, `{"k1":""}`, resp)

	resp = setKey(t, url, "k1", "v1")

	resp = getKey(t, url, "k1")
	assert.Equal(t, `{"k1":"v1"}`, resp)

	resp = getKeys(t, url)
	assert.Equal(t, `["k1"]`, resp)

	stor.m["k2"] = "v2"
	resp = getKey(t, url, "k2")
	assert.Equal(t, `{"k2":"v2"}`, resp)

	resp = getKeys(t, url)
	keys := make([]string, 0)
	json.Unmarshal([]byte(resp), &keys)
	assert.True(t, slices.Contains(keys, "k1"))
	assert.True(t, slices.Contains(keys, "k2"))

	resp = deleteKey(t, url, "k2")
	resp = getKey(t, url, "k2")
	assert.Equal(t, `{"k2":""}`, resp)
}

type testStore struct {
	m map[string]string
}

func newTestStore() *testStore {
	return &testStore{
		m: make(map[string]string),
	}
}

func (t *testStore) Get(key string) string {
	return t.m[key]
}

func (t *testStore) Keys() []string {
	return maps.Keys(t.m)
}

func (t *testStore) Set(key, value string) error {
	t.m[key] = value
	return nil
}

func (t *testStore) Delete(key string) error {
	delete(t.m, key)
	return nil
}

func getKey(t *testing.T, url, key string) string {
	client := resty.New()
	resp, err := client.R().
		Get(fmt.Sprintf("%s/keys/%s", url, key))
	assert.NoError(t, err, "failed to GET key")

	return string(resp.Body())
}

func setKey(t *testing.T, url, key, value string) string {
	client := resty.New()
	resp, err := client.R().
		SetBody(map[string]string{key: value}).
		Post(fmt.Sprintf("%s/keys", url))

	assert.NoError(t, err, "POST request failed")

	return resp.String()
}

func deleteKey(t *testing.T, url, key string) string {
	client := resty.New()
	resp, err := client.R().
		SetHeader("Accept", "application/json").
		Delete(fmt.Sprintf("%s/keys/%s", url, key))
	assert.NoError(t, err, "failed to Delete key")

	return string(resp.Body())
}

func getKeys(t *testing.T, url string) string {
	client := resty.New()
	resp, err := client.R().
		Get(fmt.Sprintf("%s/keys", url))
	assert.NoError(t, err, "failed to Get keys")

	return resp.String()
}
