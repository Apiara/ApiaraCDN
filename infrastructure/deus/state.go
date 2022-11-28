package deus

import (
	"context"
	"fmt"
	"strconv"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/go-redis/redis/v8"
)

const (
	RedisContentToServerListPrefix = "deus:location:cid:"
	RedisContentPullStatusPrefix   = "deus:dynamic:cid:sid:"
)

/*
ContentState allows changing/viewing of what content is being served on
what session servers in the network
*/
type ContentLocationIndex interface {
	Set(cid string, serverID string, dynamic bool) error
	Remove(cid string, serverID string) error
	IsServedByServer(cid string, serverID string) (bool, error)
	IsBeingServed(cid string) (bool, error)
	WasDynamicallySet(cid string, serverID string) (bool, error)
}

// mockContentState is a mock implementation for testing
type mockContentLocationIndex struct {
	serveSet map[string]struct{}
}

func (m *mockContentLocationIndex) Set(cid string, server string, dyn bool) error {
	m.serveSet[cid+server] = struct{}{}
	return nil
}

func (m *mockContentLocationIndex) Remove(cid string, server string) error {
	delete(m.serveSet, cid+server)
	return nil
}

func (m *mockContentLocationIndex) IsBeingServed(cid string) (bool, error) {
	return false, nil
}

func (m *mockContentLocationIndex) IsServedByServer(cid string, server string) (bool, error) {
	_, ok := m.serveSet[cid+server]
	return ok, nil
}

func (m *mockContentLocationIndex) WasDynamicallySet(string, string) (bool, error) { return true, nil }

// RedisContentState implements ContentState using Redis
type RedisContentLocationIndex struct {
	rdb *redis.Client
	ctx context.Context
}

// NewRedisContentState creates a new RedisContentState using address 'addr'
func NewRedisContentLocationIndex(addr string) *RedisContentLocationIndex {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})

	return &RedisContentLocationIndex{
		rdb: client,
		ctx: context.Background(),
	}
}

func (r *RedisContentLocationIndex) Set(cid string, serverID string, dynamic bool) error {
	safeCid := infra.URLToSafeName(cid)
	safeSid := infra.URLToSafeName(serverID)

	cidKey := RedisContentToServerListPrefix + safeCid
	_, err := r.rdb.SAdd(r.ctx, cidKey, serverID).Result()
	if err != nil {
		return fmt.Errorf("Failed to update list of servers serving content: %w", err)
	}

	dynKey := RedisContentPullStatusPrefix + safeCid + ":" + safeSid
	_, err = r.rdb.Set(r.ctx, dynKey, strconv.FormatBool(dynamic), 0).Result()
	if err != nil {
		return fmt.Errorf("Failed to update dynamic status key for content+server pair: %w", err)
	}
	return nil
}

func (r *RedisContentLocationIndex) Remove(cid string, serverID string) error {
	safeCid := infra.URLToSafeName(cid)
	safeSid := infra.URLToSafeName(serverID)

	cidKey := RedisContentToServerListPrefix + safeCid
	_, err := r.rdb.SRem(r.ctx, cidKey, serverID).Result()
	if err != nil {
		return fmt.Errorf("Failed to remove server from list of %s content servers: %w", cid, err)
	}

	dynKey := RedisContentPullStatusPrefix + safeCid + ":" + safeSid
	_, err = r.rdb.Del(r.ctx, dynKey).Result()
	if err != nil {
		return fmt.Errorf("Failed to remove dynamic status key for content+server pair: %w", err)
	}
	return nil
}

func (r *RedisContentLocationIndex) IsServedByServer(cid string, serverID string) (bool, error) {
	safeCid := infra.URLToSafeName(cid)
	cidKey := RedisContentToServerListPrefix + safeCid
	exists, err := r.rdb.SIsMember(r.ctx, cidKey, serverID).Result()
	if err != nil {
		return false, fmt.Errorf("Failed to lookup serve status: %w", err)
	}
	return exists, nil
}

func (r *RedisContentLocationIndex) IsBeingServed(cid string) (bool, error) {
	safeCid := infra.URLToSafeName(cid)
	cidKey := RedisContentToServerListPrefix + safeCid
	count, err := r.rdb.Exists(r.ctx, cidKey).Result()
	if err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("Failed to check if being served: %w", err)
	}
	return count > 0, nil
}

func (r *RedisContentLocationIndex) WasDynamicallySet(cid string, serverID string) (bool, error) {
	safeCid := infra.URLToSafeName(cid)
	safeSid := infra.URLToSafeName(serverID)
	dynKey := RedisContentPullStatusPrefix + safeCid + ":" + safeSid
	result, err := r.rdb.Get(r.ctx, dynKey).Result()
	if err != nil {
		return false, fmt.Errorf("Failed to check if was dynamically set: %w", err)
	}

	dyn, err := strconv.ParseBool(result)
	if err != nil {
		return false, fmt.Errorf("Failed to parse dynamic status bool result %s", result)
	}
	return dyn, nil
}
