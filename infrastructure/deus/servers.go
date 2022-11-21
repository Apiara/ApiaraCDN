package deus

import (
  "context"
  "fmt"
  "github.com/go-redis/redis/v8"
)

// GeoServerIndex provides lookup of session server for a particular region
type GeoServerIndex interface {
  GetAddress(location string) (string, error)
  SetRegionAddress(location string, address string) error
  RemoveRegionAddress(location string) error
}

// RedisGeoServerIndex implements GeoServerIndex using a Redis cache
type RedisGeoServerIndex struct {
  rdb *redis.Client
  ctx context.Context
}

// NewRedisGeoServerIndex creates a new RedisGeoServerIndex
func NewRedisGeoServerIndex(addr string) *RedisGeoServerIndex {
  client := redis.NewClient(&redis.Options{
    Addr: addr,
    Password: "",
    DB: 0,
  })

  return &RedisGeoServerIndex{
    rdb: client,
    ctx: context.Background(),
  }
}

// GetAddress retrieves a server address for a given location
func (r *RedisGeoServerIndex) GetAddress(location string) (string, error) {
  val, err := r.rdb.Get(r.ctx, location).Result()
  if err == redis.Nil {
    return "", fmt.Errorf("No server entry for location %s", location)
  } else if err != nil {
    return "", fmt.Errorf("Failed to get server entry for location %s: %w", location, err)
  }
  return val, nil
}

// SetRegionAddress adds a (location, address) pair to the index
func (r *RedisGeoServerIndex) SetRegionAddress(location string, address string) error {
  err := r.rdb.Set(r.ctx, location, address, 0).Err()
  if err != nil {
    return fmt.Errorf("Failed to create location->server entry for %s->%s: %w", location, address, err)
  }
  return nil
}

// RemoveRegionAddress removes a (location, address) pair from the index
func (r *RedisGeoServerIndex) RemoveRegionAddress(location string, address string) error {
  err := r.rdb.Del(r.ctx, location).Err()
  if err != nil {
    return fmt.Errorf("Failed to remove location->address entry for %s: %w", location, err)
  }
  return nil
}
