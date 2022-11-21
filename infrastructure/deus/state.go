package deus

import (
  "context"
  "github.com/go-redis/redis/v8"
  "fmt"
  "strconv"
)

/* ContentState allows changing/viewing of what content is being served on
what session servers in the network */
type ContentState interface {
  Set(cid string, serverID string, dynamic bool) error
  Remove(cid string, serverID string) error
  IsBeingServed(cid string, serverID string) (bool, error)
  WasDynamicallySet(cid string, serverID string) (bool, error)
}

// RedisContentState implements ContentState using Redis
type RedisContentState struct {
  rdb *redis.Client
  ctx context.Context
}

// NewRedisContentState creates a new RedisContentState using address 'addr'
func NewRedisContentState(addr string) *RedisContentState {
  client := redis.NewClient(&redis.Options{
    Addr: addr,
    Password: "",
    DB: 0,
  })

  return &RedisContentState{
    rdb: client,
    ctx: context.Background(),
  }
}

/* Set sets that serverID is currently serving cid and that this was either
invoked in a push(!dynamic) or pull(dynamic) fashion */
func (r *RedisContentState) Set(cid string, serverID string, dynamic bool) error {
  key := generateServePairKey(cid, serverID)
  err := r.rdb.Set(r.ctx, key, strconv.FormatBool(dynamic), 0).Err()
  if err != nil {
    return fmt.Errorf("Failed to create serving entry for %s on %s: %w", cid, serverID, err)
  }
  return nil
}

/* Remove sets that serverID is not serving cid anymore and that this was either
invoked in a push(!dynamic) or pull(dynamic) fashion */
func (r *RedisContentState) Remove(cid string, serverID string) error {
  key := generateServePairKey(cid, serverID)
  err := r.rdb.Del(r.ctx, key).Err()
  if err != nil {
    return fmt.Errorf("Failed to delete serving entry for %s on %s: %w", cid, serverID, err)
  }
  return nil
}

// IsBeingServed returns whether or not serverID is serving cid
func (r *RedisContentState) IsBeingServed(cid string, serverID string) (bool, error) {
  key := generateServePairKey(cid, serverID)
  _, err := r.rdb.Get(r.ctx, key).Result()
  if err == redis.Nil {
    return false, nil
  } else if err != nil {
    return false, fmt.Errorf("Failed to retrieve state: %w", err)
  }
  return true, nil
}

// WasDynamicallySet returns cid was dynamically or manually set to be served by serverID
func (r *RedisContentState) WasDynamicallySet(cid string, serverID string) (bool, error) {
  key := generateServePairKey(cid, serverID)
  val, err := r.rdb.Get(r.ctx, key).Result()
  if err == redis.Nil {
    return false, fmt.Errorf("No entry for CID: %s and ServerID: %s", cid, serverID)
  } else if err != nil {
    return false, fmt.Errorf("Failed to retrieve state: %w", err)
  }

  result, _ := strconv.ParseBool(val)
  return result, nil
}