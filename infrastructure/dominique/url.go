package dominique

import (
  "fmt"
  "context"
  "github.com/go-redis/redis/v8"
  infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

// URLIndex allows looking up what URL a Functional ID is linked to
type URLIndex interface {
  FunctionalIDToURL(string) (string, error)
}

// mockURLIndex is a testing mock for URLIndex
type mockURLIndex struct{}
func (m *mockURLIndex) FunctionalIDToURL(s string) (string, error) {
  return "url_" + s, nil
}

// RedisURLIndex looks up the URL info in the redis microservice state database
type RedisURLIndex struct {
  rdb *redis.Client
  ctx context.Context
}

// NewRedisURLIndex creates a new redisURLIndex
func NewRedisURLIndex(addr string) *RedisURLIndex {
  client := redis.NewClient(&redis.Options{
    Addr: addr,
    Password: "",
    DB: 0,
  })

  return &RedisURLIndex{
    rdb: client,
    ctx: context.Background(),
  }
}

// FunctionalIDToURL attempts to map a FID to URL
func (r *RedisURLIndex) FunctionalIDToURL(fid string) (string, error) {
  urlLookupKey := infra.RedisFunctionalToURLKey + fid
  url, err := r.rdb.Get(r.ctx, urlLookupKey).Result()
  if err != nil {
    return "", fmt.Errorf("Failed to retrieve url from functional id %s: %w", fid, err)
  }
  return url, nil
}
