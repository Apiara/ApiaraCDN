package infrastructure

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/go-redis/redis/v8"
)

const (
	// State Key mapping FIDs to Plaintext URLs
	RedisFunctionalToURLKey = "infra:content:functional:"

	/* State Key mapping Safe URLs to FIDs. Note URLs must be encoded in a safe format
	that doesn't include special characters, specifically ":" */
	RedisURLToFunctionalKey = "infra:content:url:"

	// State Key mapping Safe URLs to a set of all filesystem resources created under URL
	RedisURLToResourcesKey = "infra:content:resources:"

	// State Key mapping Safe URLs to Byte Size for content
	RedisURLToByteSizeKey = "infra:content:size:"
)

// DataIndexReader exposes all read functions for a DataIndex
type DataIndexReader interface {
	GetFunctionalID(cid string) (string, error)
	GetContentID(fid string) (string, error)
	GetResources(cid string) ([]string, error)
	GetSize(cid string) (int64, error)
}

// DataIndexWriter exposes all write functions for a DataIndex
type DataIndexWriter interface {
	Create(cid string, fid string, size int64, resources []string) error
	Delete(cid string) error
}

/*
DataIndex represents an object that keep track of information regarding
data being served on the network such as the different names the data goes
by, the net size of the data, and the resource files associated with the data
*/
type DataIndex interface {
	DataIndexReader
	DataIndexWriter
}

// Testing mock for DataIndex
type MockDataIndex struct {
	cidMap      map[string]string
	fidMap      map[string]string
	sizeMap     map[string]int64
	resourceMap map[string][]string
}

func NewMockDataIndex() *MockDataIndex {
	return &MockDataIndex{
		cidMap:      make(map[string]string),
		fidMap:      make(map[string]string),
		sizeMap:     make(map[string]int64),
		resourceMap: make(map[string][]string),
	}
}

func (m *MockDataIndex) Create(cid string, fid string, size int64, resources []string) error {
	m.cidMap[fid] = cid
	m.fidMap[cid] = fid
	m.sizeMap[cid] = size
	m.resourceMap[cid] = resources
	return nil
}

func (m *MockDataIndex) Delete(cid string) error {
	if _, ok := m.sizeMap[cid]; ok {
		fid := m.fidMap[cid]

		delete(m.cidMap, fid)
		delete(m.fidMap, cid)
		delete(m.sizeMap, cid)
		delete(m.resourceMap, cid)
	}
	return nil
}

func (m *MockDataIndex) GetFunctionalID(cid string) (string, error) {
	if fid, ok := m.fidMap[cid]; ok {
		return fid, nil
	}
	return "", fmt.Errorf("No key %s", cid)
}

func (m *MockDataIndex) GetContentID(fid string) (string, error) {
	if cid, ok := m.cidMap[fid]; ok {
		return cid, nil
	}
	return "", fmt.Errorf("No fid key %s", fid)
}

func (m *MockDataIndex) GetResources(cid string) ([]string, error) {
	if resources, ok := m.resourceMap[cid]; ok {
		return resources, nil
	}
	return nil, fmt.Errorf("No key %s", cid)
}

func (m *MockDataIndex) GetSize(cid string) (int64, error) {
	if size, ok := m.sizeMap[cid]; ok {
		return size, nil
	}
	return -1, fmt.Errorf("No key %s", cid)
}

// RedisDataIndex implements DataIndex using redis for storage
type RedisDataIndex struct {
	rdb *redis.Client
	ctx context.Context
}

// NewRedisDataIndex creates a new RedisDataIndex
func NewRedisDataIndex(addr string) *RedisDataIndex {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	return &RedisDataIndex{
		rdb: client,
		ctx: context.Background(),
	}
}

func (n *RedisDataIndex) GetResources(cid string) ([]string, error) {
	urlKey := URLToSafeName(cid)
	resourceMapKey := RedisURLToResourcesKey + urlKey
	resources, err := n.rdb.SMembers(n.ctx, resourceMapKey).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("No resources found under ID %s", cid)
	} else if err != nil {
		return nil, fmt.Errorf("Failed to read resources list for ID %s: %w", cid, err)
	}
	return resources, nil
}

func (n *RedisDataIndex) GetContentID(fid string) (string, error) {
	fidMapKey := RedisFunctionalToURLKey + fid
	cid, err := n.rdb.Get(n.ctx, fidMapKey).Result()

	if err == redis.Nil {
		return "", fmt.Errorf("Failed to get content id for functional id %s. Doesn't exist", fid)
	} else if err != nil {
		return "", fmt.Errorf("Failed to get content id for functional id %s: %w", fid, err)
	}
	return cid, nil
}

func (n *RedisDataIndex) GetFunctionalID(cid string) (string, error) {
	cidKey := URLToSafeName(cid)
	cidMapKey := RedisURLToFunctionalKey + cidKey
	fid, err := n.rdb.Get(n.ctx, cidMapKey).Result()

	if err == redis.Nil {
		return "", fmt.Errorf("Failed to get functional id for content id %s. Doesn't exist", cid)
	} else if err != nil {
		return "", fmt.Errorf("Failed to get functional id for content id %s: %w", cid, err)
	}
	return fid, nil
}

func (n *RedisDataIndex) Create(cid string, fid string, size int64, resources []string) error {
	// Create FunctionalID to URL mapping
	fidMapKey := RedisFunctionalToURLKey + fid
	err := n.rdb.Set(n.ctx, fidMapKey, cid, 0).Err()
	if err != nil {
		return fmt.Errorf("Failed to add functional id to url mapping: %w", err)
	}

	// Create URL to FunctionalID mapping
	cidKey := URLToSafeName(cid)
	urlMapKey := RedisURLToFunctionalKey + cidKey
	if err := n.rdb.Set(n.ctx, urlMapKey, fid, 0).Err(); err != nil {
		return fmt.Errorf("Failed to add url to functional id mapping: %w", err)
	}

	// Create URL to size mapping
	sizeKey := RedisURLToByteSizeKey + cidKey
	if err := n.rdb.Set(n.ctx, sizeKey, strconv.FormatInt(size, 10), 0).Err(); err != nil {
		return fmt.Errorf("Failed to add url to byte size mapping: %w", err)
	}

	// Create URL to Resources mapping
	resourceMapKey := RedisURLToResourcesKey + cidKey
	for _, resource := range resources {
		val, err := n.rdb.SAdd(n.ctx, resourceMapKey, resource).Result()
		if err != nil {
			return fmt.Errorf("Failed to add url to resources mapping: %w", err)
		} else if val != 1 {
			return fmt.Errorf("Failed to add url to resources mapping")
		}
	}
	return nil
}

func (n *RedisDataIndex) Delete(cid string) error {
	fid, err := n.GetFunctionalID(cid)
	if err != nil {
		return fmt.Errorf("Failed to delete %s: %w", cid, err)
	}

	cidKey := URLToSafeName(cid)
	resourceMapKey := RedisURLToResourcesKey + cidKey
	cidMapKey := RedisURLToFunctionalKey + cidKey
	fidMapKey := RedisFunctionalToURLKey + fid
	sizeMapKey := RedisURLToByteSizeKey + cidKey

	if err = n.rdb.Del(n.ctx, resourceMapKey).Err(); err != nil {
		return fmt.Errorf("Failed to remove resource mapping: %w", err)
	}
	if err = n.rdb.Del(n.ctx, cidMapKey).Err(); err != nil {
		return fmt.Errorf("Failed to remove content id to functional id mapping: %w", err)
	}
	if err = n.rdb.Del(n.ctx, fidMapKey).Err(); err != nil {
		return fmt.Errorf("Failed to remove functional id to content id mapping: %w", err)
	}
	if err = n.rdb.Del(n.ctx, sizeMapKey).Err(); err != nil {
		return fmt.Errorf("Failed to remove content id to size mapping: %w", err)
	}
	return nil
}

func (n *RedisDataIndex) GetSize(cid string) (int64, error) {
	cidKey := URLToSafeName(cid)
	sizeKey := RedisURLToByteSizeKey + cidKey

	sizeStr, err := n.rdb.Get(n.ctx, sizeKey).Result()
	if err != nil {
		return -1, fmt.Errorf("Failed to get size for %s: %w", cid, err)
	}
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("Failed to parse size for %s: %w", cid, err)
	}
	return size, nil
}

/*
URLToSafeName converts URL with possible unsafe
characters to a unique hex string 24 bytes long
*/
func URLToSafeName(url string) string {
	sum := sha256.Sum224([]byte(url))
	safe := hex.EncodeToString(sum[:])
	return safe
}
