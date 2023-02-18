package state

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/go-redis/redis/v8"
)

const (
	unassigned = "(unassigned)"
)

var (
	ErrNilState = errors.New("microservice state nil")
)

type ServerStateWriter interface {
	CreateServerEntry(sid string, publicAddr string, privateAddr string) error
	DeleteServerEntry(sid string) error
}

type ServerStateReader interface {
	GetServerPublicAddress(sid string) (string, error)
	GetServerPrivateAddress(sid string) (string, error)
}

type ServerState interface {
	ServerStateWriter
	ServerStateReader
}

type ContentMetadataStateReader interface {
	GetContentFunctionalID(cid string) (string, error)
	GetContentID(fid string) (string, error)
	GetContentResources(cid string) ([]string, error)
	GetContentSize(cid string) (int64, error)
}

type ContentMetadataStateWriter interface {
	CreateContentEntry(cid string, fid string, size int64, resources []string) error
	DeleteContentEntry(cid string) error
}

/*
ContentMetadataState represents and object that can read/write information
about content being served by the network
*/
type ContentMetadataState interface {
	ContentMetadataStateReader
	ContentMetadataStateWriter
}

type ContentLocationStateReader interface {
	IsContentServedByServer(cid string, serverID string) (bool, error)
	ContentServerList(cid string) ([]string, error)
	ServerContentList(serverID string) ([]string, error)
	IsContentBeingServed(cid string) (bool, error)
	WasContentPulled(cid string, serverID string) (bool, error)
}

type ContentLocationStateWriter interface {
	CreateContentLocationEntry(cid string, serverID string, pulled bool) error
	DeleteContentLocationEntry(cid string, serverID string) error
}

type ContentLocationState interface {
	ContentLocationStateReader
	ContentLocationStateWriter
}

type ContentPullRuleStateReader interface {
	GetContentPullRules() ([]string, error)
	ContentPullRuleExists(rule string) (bool, error)
}

type ContentPullRuleStateWriter interface {
	CreateContentPullRule(rule string) error
	DeleteContentPullRule(rule string) error
}

type ContentPullRuleState interface {
	ContentPullRuleStateReader
	ContentPullRuleStateWriter
}

/*
MicroserviceState represents an object that can be used to
read/write to the shared microservice state safely
*/
type MicroserviceState interface {
	// Content information
	ContentMetadataState

	// Server information
	ServerState

	// Content location information
	ServerList() ([]string, error)
	ContentLocationState

	// Content pull rules
	ContentPullRuleState
}

const (
	RedisKeyDelimiter = ":"

	// Content metadata tables
	RedisContentMetadataTable         = "content:"
	RedisContentMetadataFIDAttr       = ":fid"
	RedisContentMetadataSizeAttr      = ":size"
	RedisContentMetadataResourcesAttr = ":resources"
	RedisContentMetadataLocationAttr  = ":location"

	RedisContentMetadataReverseTable   = "content:reverse:"
	RedisContentMetadataReverseCIDAttr = ":cid"

	// Content location on edge network tables
	RedisContentEdgeServerTable           = "edge:"
	RedisContentEdgeServerServingAttr     = ":serving"
	RedisContentEdgeServerPublicAddrAttr  = ":public"
	RedisContentEdgeServerPrivateAddrAttr = ":private"

	// Content serve mechanism tracker
	RedisContentServeMechanismTable      = "mechanism:"
	RedisContentServeMechanismPulledAttr = ":pulled"

	// Content pull rules table
	RedisContentPullRulesList = "rules:list"
)

// RedisMicroserviceState implements MicroserviceConfiguration using Redis
type RedisMicroserviceState struct {
	rdb   *redis.Client
	ctx   context.Context
	mutex *sync.RWMutex
}

/*
NewRedisMicroserviceState creates a new instance of RedisMicroserviceState
referencing the redis instance at addr
*/
func NewRedisMicroserviceState(addr string) *RedisMicroserviceState {
	return &RedisMicroserviceState{
		rdb: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: "",
			DB:       0,
		}),
		ctx:   context.Background(),
		mutex: &sync.RWMutex{},
	}
}

// CreateContentEntry creates a metadata entry for a piece of content
func (r *RedisMicroserviceState) CreateContentEntry(cid string, fid string, size int64, resources []string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Create attribute list to write
	safeCid := infra.URLToSafeName(cid)
	fidKey := RedisContentMetadataTable + safeCid + RedisContentMetadataFIDAttr
	sizeKey := RedisContentMetadataTable + safeCid + RedisContentMetadataSizeAttr
	resourcesKey := RedisContentMetadataTable + safeCid + RedisContentMetadataResourcesAttr
	cidKey := RedisContentMetadataReverseTable + fid + RedisContentMetadataReverseCIDAttr

	// Write forward attributes
	pipe := r.rdb.TxPipeline()
	errMsg := "failed to create content entry for %s: %w"
	if err := pipe.Set(r.ctx, fidKey, fid, 0).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	if err := pipe.Set(r.ctx, sizeKey, strconv.FormatInt(size, 10), 0).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	for _, resource := range resources {
		err := pipe.SAdd(r.ctx, resourcesKey, resource).Err()
		if err != nil {
			return fmt.Errorf(errMsg, cid, err)
		}
	}

	// Write reverse attributes
	if err := pipe.Set(r.ctx, cidKey, cid, 0).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}

	// Execute transaction
	if _, err := pipe.Exec(r.ctx); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	return nil
}

func (r *RedisMicroserviceState) propagateContentDeletion(pipe redis.Pipeliner, cid string) error {
	locationKey := RedisContentMetadataTable + infra.URLToSafeName(cid) + RedisContentMetadataLocationAttr
	servers, err := pipe.SMembers(r.ctx, locationKey).Result()
	if err != nil {
		return err
	}

	for _, serverID := range servers {
		if err := r.txDeleteContentLocationEntry(pipe, cid, serverID); err != nil {
			return err
		}
	}
	return nil
}

// DeleteContentEntry removes a metadata entry for a piece of content
func (r *RedisMicroserviceState) DeleteContentEntry(cid string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Create attribute list to delete
	safeCid := infra.URLToSafeName(cid)
	fidKey := RedisContentMetadataTable + safeCid + RedisContentMetadataFIDAttr
	sizeKey := RedisContentMetadataTable + safeCid + RedisContentMetadataSizeAttr
	resourcesKey := RedisContentMetadataTable + safeCid + RedisContentMetadataResourcesAttr

	// Read fid and create reverse cid lookup attribute
	errMsg := "failed to delete content entry for %s: %w"
	fid, err := r.rdb.Get(r.ctx, fidKey).Result()
	if err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	cidKey := RedisContentMetadataReverseTable + fid + RedisContentMetadataReverseCIDAttr

	// Delete forward attributes
	pipe := r.rdb.TxPipeline()
	if err := pipe.Del(r.ctx, fidKey).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	if err := pipe.Del(r.ctx, sizeKey).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	if err := pipe.Del(r.ctx, resourcesKey).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}

	// Delete reverse attributes
	if err := pipe.Del(r.ctx, cidKey).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}

	// Delete references from foreign tables
	r.propagateContentDeletion(pipe, cid)

	// Execute transaction
	if _, err := pipe.Exec(r.ctx); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	return nil
}

// GetContentFunctionalID retrieves the functional ID for a given content ID
func (r *RedisMicroserviceState) GetContentFunctionalID(cid string) (string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	safeCid := infra.URLToSafeName(cid)
	fidKey := RedisContentMetadataTable + safeCid + RedisContentMetadataFIDAttr

	cid, err := r.rdb.Get(r.ctx, fidKey).Result()
	if err != nil {
		return "", fmt.Errorf("failed to get functional ID for content(%s): %w", cid, err)
	}
	return cid, nil
}

// GetContentID retrieves a content ID given and functional ID
func (r *RedisMicroserviceState) GetContentID(fid string) (string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	cidKey := RedisContentMetadataReverseTable + fid + RedisContentMetadataReverseCIDAttr
	cid, err := r.rdb.Get(r.ctx, cidKey).Result()
	if err != nil {
		return "", fmt.Errorf("failed to get content from functional ID(%s): %w", fid, err)
	}
	return cid, nil
}

// GetContentResources retrieves resource names associated with a content ID
func (r *RedisMicroserviceState) GetContentResources(cid string) ([]string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	safeCid := infra.URLToSafeName(cid)
	resourcesKey := RedisContentMetadataTable + safeCid + RedisContentMetadataResourcesAttr

	resources, err := r.rdb.SMembers(r.ctx, resourcesKey).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("no resources found under %s", cid)
	} else if err != nil {
		return nil, fmt.Errorf("failed to read resources list for content(%s): %w", cid, err)
	}
	return resources, nil
}

// GetContentSize retrieves the content size associated with a content ID
func (r *RedisMicroserviceState) GetContentSize(cid string) (int64, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	safeCid := infra.URLToSafeName(cid)
	sizeKey := RedisContentMetadataTable + safeCid + RedisContentMetadataSizeAttr

	sizeStr, err := r.rdb.Get(r.ctx, sizeKey).Result()
	if err == redis.Nil {
		return -1, fmt.Errorf("no size found under content(%s)", cid)
	} else if err != nil {
		return -1, fmt.Errorf("failed to get size for content(%s): %w", cid, err)
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("failed to parse size value for content(%s): %s", cid, sizeStr)
	}
	return size, nil
}

// CreateContentLocationEntry updates the datastore to indicate a content ID is being served by a server
func (r *RedisMicroserviceState) CreateContentLocationEntry(cid string, serverID string, pulled bool) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Create all key names in tables
	servingKey := RedisContentEdgeServerTable + serverID + RedisContentEdgeServerServingAttr
	locationKey := RedisContentMetadataTable + infra.URLToSafeName(cid) + RedisContentMetadataLocationAttr
	mechanismKey := RedisContentServeMechanismTable + infra.URLToSafeName(cid) +
		RedisKeyDelimiter + serverID + RedisContentServeMechanismPulledAttr

	// Create add transaction
	errMsg := "failed to perform add update on content(%s)/location(%s): %w"
	pipe := r.rdb.TxPipeline()
	if err := pipe.SAdd(r.ctx, servingKey, cid).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, serverID, err)
	}
	if err := pipe.SAdd(r.ctx, locationKey, serverID).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, serverID, err)
	}
	if err := pipe.Set(r.ctx, mechanismKey, strconv.FormatBool(pulled), 0).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, serverID, err)
	}

	// Execute transaction
	if _, err := pipe.Exec(r.ctx); err != nil {
		return fmt.Errorf(errMsg, cid, serverID, err)
	}
	return nil
}

func (r *RedisMicroserviceState) txDeleteContentLocationEntry(pipe redis.Pipeliner, cid string, serverID string) error {
	// Create all key names in tables
	servingKey := RedisContentEdgeServerTable + serverID + RedisContentEdgeServerServingAttr
	locationKey := RedisContentMetadataTable + infra.URLToSafeName(cid) + RedisContentMetadataLocationAttr
	mechanismKey := RedisContentServeMechanismTable + infra.URLToSafeName(cid) +
		RedisKeyDelimiter + serverID + RedisContentServeMechanismPulledAttr

	// Create deletion transaction
	errMsg := "failed to perform deletion update on content(%s)/location(%s): %w"
	if err := pipe.SRem(r.ctx, servingKey, cid).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, serverID, err)
	}
	if err := pipe.SRem(r.ctx, locationKey, serverID).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, serverID, err)
	}
	if err := pipe.Del(r.ctx, mechanismKey).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, serverID, err)
	}
	return nil
}

// DeleteContentLocationEntry updates the data store so a server is no longer serving a content ID
func (r *RedisMicroserviceState) DeleteContentLocationEntry(cid string, serverID string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	pipe := r.rdb.TxPipeline()
	r.txDeleteContentLocationEntry(pipe, cid, serverID)
	if _, err := pipe.Exec(r.ctx); err != nil {
		return err
	}
	return nil
}

func (r *RedisMicroserviceState) CreateServerEntry(sid string, publicAddr string, privateAddr string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// If missing parameter, set to microservice state 'unassigned' value
	if publicAddr == "" {
		publicAddr = unassigned
	}
	if privateAddr == "" {
		privateAddr = unassigned
	}

	// Create keys
	serverKeyBase := RedisContentEdgeServerTable + sid
	publicAddrKey := serverKeyBase + RedisContentEdgeServerPublicAddrAttr
	privateAddrKey := serverKeyBase + RedisContentEdgeServerPrivateAddrAttr

	errMsg := "failed to create server(%s) entry: %w"
	pipe := r.rdb.TxPipeline()
	if err := pipe.Set(r.ctx, publicAddrKey, publicAddr, 0).Err(); err != nil {
		return fmt.Errorf(errMsg, sid, err)
	}
	if err := pipe.Set(r.ctx, privateAddrKey, privateAddr, 0).Err(); err != nil {
		return fmt.Errorf(errMsg, sid, err)
	}
	if _, err := pipe.Exec(r.ctx); err != nil {
		return fmt.Errorf(errMsg, sid, err)
	}
	return nil
}

func (r *RedisMicroserviceState) DeleteServerEntry(sid string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	serverKeyBase := RedisContentEdgeServerTable + sid
	publicAddrKey := serverKeyBase + RedisContentEdgeServerPublicAddrAttr
	privateAddrKey := serverKeyBase + RedisContentEdgeServerPrivateAddrAttr
	contentListKey := serverKeyBase + RedisContentEdgeServerServingAttr

	errMsg := "failed to delete server(%s) entry: %w"
	pipe := r.rdb.TxPipeline()

	// Get list of all content server is serving
	contentList, err := pipe.SMembers(r.ctx, contentListKey).Result()
	if err != nil {
		return fmt.Errorf(errMsg, sid, err)
	}

	// Delete all keys from edge server table
	err = pipe.Del(r.ctx, publicAddrKey, privateAddrKey, contentListKey).Err()
	if err != nil {
		return fmt.Errorf(errMsg, sid, err)
	}

	// Delete server ID from serving lists for individual pieces of content
	for _, contentID := range contentList {
		locationKey := RedisContentMetadataTable + infra.URLToSafeName(contentID) + RedisContentMetadataLocationAttr
		if err = pipe.SRem(r.ctx, locationKey, sid).Err(); err != nil {
			return fmt.Errorf(errMsg, sid, err)
		}
	}

	// Execute transaction
	if _, err = pipe.Exec(r.ctx); err != nil {
		return fmt.Errorf(errMsg, sid, err)
	}
	return nil
}

func (r *RedisMicroserviceState) getServerAddress(key string, errMsg string) (string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	addr, err := r.rdb.Get(r.ctx, key).Result()
	if addr == unassigned {
		return "", ErrNilState
	} else if err != nil {
		return "", fmt.Errorf(errMsg, err)
	}
	return addr, nil

}

// Get public facing service API for the server
func (r *RedisMicroserviceState) GetServerPublicAddress(sid string) (string, error) {
	errMsg := fmt.Sprintf("failed to get public server(%s) address: ", sid) + "%w"
	publicAddrKey := RedisContentEdgeServerTable + sid + RedisContentEdgeServerPublicAddrAttr
	return r.getServerAddress(publicAddrKey, errMsg)
}

// Get the internal service API address for the server
func (r *RedisMicroserviceState) GetServerPrivateAddress(sid string) (string, error) {
	errMsg := fmt.Sprintf("failed to get private server(%s) address: ", sid) + "%w"
	privateAddrKey := RedisContentEdgeServerTable + sid + RedisContentEdgeServerPrivateAddrAttr
	return r.getServerAddress(privateAddrKey, errMsg)
}

// Get a list of all edge server IDs
func (r *RedisMicroserviceState) ServerList() ([]string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	errMsg := "failed to get server list: %w"
	edgeServers, err := r.rdb.Keys(r.ctx, RedisContentEdgeServerTable+"*"+RedisContentEdgeServerPrivateAddrAttr).Result()
	if err != nil {
		return nil, fmt.Errorf(errMsg, err)
	}

	servers := make([]string, len(edgeServers))
	for i, edgeServerKey := range edgeServers {
		keyParts := strings.Split(edgeServerKey, RedisKeyDelimiter)
		if len(keyParts) != 3 {
			return nil, fmt.Errorf(errMsg, fmt.Errorf("invalid edge key: %s", edgeServerKey))
		}
		servers[i] = keyParts[1]
	}
	return servers, nil
}

// IsContentServedByServer returns whether or not a content ID is being served by a server
func (r *RedisMicroserviceState) IsContentServedByServer(cid string, serverID string) (bool, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	servingKey := RedisContentEdgeServerTable + serverID + RedisContentEdgeServerServingAttr
	result, err := r.rdb.SIsMember(r.ctx, servingKey, cid).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check if content(%s) is served by server(%s): %w", cid, serverID, err)
	}
	return result, nil
}

func (r *RedisMicroserviceState) getContentServerList(cid string) ([]string, error) {
	locationKey := RedisContentMetadataTable + infra.URLToSafeName(cid) + RedisContentMetadataLocationAttr
	servers, err := r.rdb.SMembers(r.ctx, locationKey).Result()
	if err == redis.Nil {
		servers = []string{}
	} else if err != nil {
		return nil, fmt.Errorf("failed to get server list for content(%s): %w", cid, err)
	}
	return servers, nil
}

// ContentServerList returns the list of servers currently serving a content ID
func (r *RedisMicroserviceState) ContentServerList(cid string) ([]string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.getContentServerList(cid)
}

// ServerContentList returns the list of content a server is currently serving
func (r *RedisMicroserviceState) ServerContentList(serverID string) ([]string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	servingKey := RedisContentEdgeServerTable + serverID + RedisContentEdgeServerServingAttr
	serving, err := r.rdb.SMembers(r.ctx, servingKey).Result()
	if err == redis.Nil {
		serving = []string{}
	} else if err != nil {
		return nil, fmt.Errorf("failed to get serving list for server(%s): %w", serverID, err)
	}
	return serving, nil
}

// IsContentBeingServed returns whether or not a piece of content is being served anywhere on the network
func (r *RedisMicroserviceState) IsContentBeingServed(cid string) (bool, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	servers, err := r.getContentServerList(cid)
	if err != nil {
		return false, fmt.Errorf("failed to check if content(%s) is being served: %w", cid, err)
	}
	return len(servers) > 0, nil
}

// WasContentPulled returns whether or not a content was pulled by the network(as opposed to manually pushed to the network)
func (r *RedisMicroserviceState) WasContentPulled(cid string, serverID string) (bool, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	mechanismKey := RedisContentServeMechanismTable + infra.URLToSafeName(cid) +
		RedisKeyDelimiter + serverID + RedisContentServeMechanismPulledAttr

	errMsg := "failed to find serve mechanism of content(%s) at server(%s): %w"
	resultStr, err := r.rdb.Get(r.ctx, mechanismKey).Result()
	if err != nil {
		return false, fmt.Errorf(errMsg, cid, serverID, err)
	}

	result, err := strconv.ParseBool(resultStr)
	if err != nil {
		return false, fmt.Errorf(errMsg, cid, serverID, err)
	}
	return result, nil
}

// CreateContentPullRule stores a new rule that can be used to validate a piece of content elligibility for being pulled
func (r *RedisMicroserviceState) CreateContentPullRule(rule string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if err := r.rdb.SAdd(r.ctx, RedisContentPullRulesList, rule).Err(); err != nil {
		return fmt.Errorf("failed to add rule(%s) to rule list: %w", rule, err)
	}
	return nil
}

// DeleteContentPullRule removes a pull rule from the store
func (r *RedisMicroserviceState) DeleteContentPullRule(rule string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if err := r.rdb.SRem(r.ctx, RedisContentPullRulesList, rule).Err(); err != nil {
		return fmt.Errorf("failed to remove rule(%s) from rule list: %w", rule, err)
	}
	return nil
}

// GetContentPullRules returns all content pull rules currently in effect
func (r *RedisMicroserviceState) GetContentPullRules() ([]string, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	rules, err := r.rdb.SMembers(r.ctx, RedisContentPullRulesList).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve pull rules: %w", err)
	}
	return rules, nil
}

// ContentPullRuleExists checks if a pull rule is currently in effect
func (r *RedisMicroserviceState) ContentPullRuleExists(rule string) (bool, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result, err := r.rdb.SIsMember(r.ctx, RedisContentPullRulesList, rule).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check if rule(%s) exists: %w", rule, err)
	}
	return result, nil
}
