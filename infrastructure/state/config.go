package infrastructure

import (
	"context"
	"fmt"
	"strconv"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/go-redis/redis/v8"
)

type MicroserviceConfiguration interface {
	// Server region mapping
	GetRegionAddress(location string) (string, error)
	SetRegionAddress(location string, address string) error
	RemoveRegionAddress(location string) error

	// Content information
	GetContentFunctionalID(cid string) (string, error)
	GetContentID(fid string) (string, error)
	GetContentResources(cid string) ([]string, error)
	GetContentSize(cid string) (int64, error)
	CreateContentEntry(cid string, fid string, size int64, resources []string) error
	DeleteContentEntry(cid string) error

	// Content location information
	IsContentServedByServer(cid string, serverID string) (bool, error)
	ContentServerList(cid string) ([]string, error)
	IsContentBeingServed(cid string) (bool, error)
	WasContentPulled(cid string, serverID string) (bool, error)
	CreateContentLocationEntry(cid string, serverID string, pulled bool) error
	DeleteContentLocationEntry(cid string, serverID string) error

	// Content pull rules
	GetContentPullRules() ([]string, error)
	ContentPullRuleExists(rule string) (bool, error)
	CreateContentPullRule(rule string) error
	DeleteContentPullRule(rule string) error
}

const (
	// Region to server mapping table
	RedisRegionTable      = "region:"
	RedisRegionServerAttr = ":server"

	// Content metadata tables
	RedisContentMetadataTable         = "content:"
	RedisContentMetadataFIDAttr       = ":fid"
	RedisContentMetadataSizeAttr      = ":size"
	RedisContentMetadataResourcesAttr = ":resources"
	RedisContentMetadataLocationAttr  = ":location"

	RedisContentMetadataReverseTable   = "content:reverse:"
	RedisContentMetadataReverseCIDAttr = ":cid"

	// Content location on edge network tables
	RedisContentEdgeLocationTable       = "edge:"
	RedisContentEdgeLocationServingAttr = ":serving"

	// Content serve mechanism tracker
	RedisContentServeMechanismTable      = "mechanism:"
	RedisContentServeMechanismPulledAttr = ":pulled"

	// Content pull rules table
	RedisContentPullRulesList = "rules:list"
)

// RedisMicroserviceConfiguration implements MicroserviceConfiguration using Redis
type RedisMicroserviceConfiguration struct {
	rdb *redis.Client
	ctx context.Context
}

// GetRegionAddress retrieves the server address for the 'location'
func (r *RedisMicroserviceConfiguration) GetRegionAddress(location string) (string, error) {
	serverKey := RedisRegionTable + location + RedisRegionServerAttr
	server, err := r.rdb.Get(r.ctx, serverKey).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("no server ID for region %s", location)
	} else if err != nil {
		return "", fmt.Errorf("failed to get server ID for region(%s): %w", location, err)
	}
	return server, nil
}

// SetRegionAddress sets the server address that services a specific region
func (r *RedisMicroserviceConfiguration) SetRegionAddress(location string, address string) error {
	serverKey := RedisRegionTable + location + RedisRegionServerAttr
	err := r.rdb.Set(r.ctx, serverKey, address, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to create region(%s) to server(%s) entry: %w", location, address, err)
	}
	return nil
}

// RemoveRegionAddress removes the address associated with a region
func (r *RedisMicroserviceConfiguration) RemoveRegionAddress(location string) error {
	serverKey := RedisRegionTable + location + RedisRegionServerAttr
	if err := r.rdb.Del(r.ctx, serverKey).Err(); err != nil {
		return fmt.Errorf("failed to remove region(%s) server entry: %w", location, err)
	}
	return nil
}

// CreateContentEntry creates a metadata entry for a piece of content
func (r *RedisMicroserviceConfiguration) CreateContentEntry(cid string, fid string, size int64, resources []string) error {
	// Create attribute list to write
	safeCid := infra.URLToSafeName(cid)
	fidKey := RedisContentMetadataTable + safeCid + RedisContentMetadataFIDAttr
	sizeKey := RedisContentMetadataTable + safeCid + RedisContentMetadataSizeAttr
	resourcesKey := RedisContentMetadataTable + safeCid + RedisContentMetadataResourcesAttr
	cidKey := RedisContentMetadataReverseTable + fid + RedisContentMetadataReverseCIDAttr

	// Write forward attributes
	pipe := r.rdb.TxPipeline()
	errMsg := "Failed to create content entry for %s: :%w"
	if err := pipe.Set(r.ctx, fidKey, fid, 0).Err(); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	if err := pipe.Set(r.ctx, sizeKey, strconv.FormatInt(size, 10), 0); err != nil {
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

func (r *RedisMicroserviceConfiguration) propagateContentDeletion(pipe redis.Pipeliner, cid string) error {
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
func (r *RedisMicroserviceConfiguration) DeleteContentEntry(cid string) error {
	// Create attribute list to delete
	safeCid := infra.URLToSafeName(cid)
	fidKey := RedisContentMetadataTable + safeCid + RedisContentMetadataFIDAttr
	sizeKey := RedisContentMetadataTable + safeCid + RedisContentMetadataSizeAttr
	resourcesKey := RedisContentMetadataTable + safeCid + RedisContentMetadataResourcesAttr

	// Read fid and create reverse cid lookup attribute
	errMsg := "Failed to delete content entry for %s: :%w"
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
func (r *RedisMicroserviceConfiguration) GetContentFunctionalID(cid string) (string, error) {
	safeCid := infra.URLToSafeName(cid)
	fidKey := RedisContentMetadataTable + safeCid + RedisContentMetadataFIDAttr

	cid, err := r.rdb.Get(r.ctx, fidKey).Result()
	if err != nil {
		return "", fmt.Errorf("failed to get functional ID for content(%s): %w", cid, err)
	}
	return cid, nil
}

// GetContentID retrieves a content ID given and functional ID
func (r *RedisMicroserviceConfiguration) GetContentID(fid string) (string, error) {
	cidKey := RedisContentMetadataReverseTable + fid + RedisContentMetadataReverseCIDAttr

	cid, err := r.rdb.Get(r.ctx, cidKey).Result()
	if err != nil {
		return "", fmt.Errorf("failed to get content from functional ID(%s): %w", fid, err)
	}
	return cid, nil
}

// GetContentResources retrieves resource names associated with a content ID
func (r *RedisMicroserviceConfiguration) GetContentResources(cid string) ([]string, error) {
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
func (r *RedisMicroserviceConfiguration) GetContentSize(cid string) (int64, error) {
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
func (r *RedisMicroserviceConfiguration) CreateContentLocationEntry(cid string, serverID string, pulled bool) error {
	// Create all key names in tables
	servingKey := RedisContentEdgeLocationTable + infra.URLToSafeName(serverID) + RedisContentEdgeLocationServingAttr
	locationKey := RedisContentMetadataTable + infra.URLToSafeName(cid) + RedisContentMetadataLocationAttr
	mechanismKey := RedisContentServeMechanismTable + infra.URLToSafeName(cid) +
		infra.URLToSafeName(serverID) + RedisContentServeMechanismPulledAttr

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

func (r *RedisMicroserviceConfiguration) txDeleteContentLocationEntry(pipe redis.Pipeliner, cid string, serverID string) error {
	// Create all key names in tables
	servingKey := RedisContentEdgeLocationTable + infra.URLToSafeName(serverID) + RedisContentEdgeLocationServingAttr
	locationKey := RedisContentMetadataTable + infra.URLToSafeName(cid) + RedisContentMetadataLocationAttr
	mechanismKey := RedisContentServeMechanismTable + infra.URLToSafeName(cid) +
		infra.URLToSafeName(serverID) + RedisContentServeMechanismPulledAttr

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
func (r *RedisMicroserviceConfiguration) DeleteContentLocationEntry(cid string, serverID string) error {
	pipe := r.rdb.TxPipeline()
	r.txDeleteContentLocationEntry(pipe, cid, serverID)
	if _, err := pipe.Exec(r.ctx); err != nil {
		return err
	}
	return nil
}

// IsContentServedByServer returns whether or not a content ID is being served by a server
func (r *RedisMicroserviceConfiguration) IsContentServedByServer(cid string, serverID string) (bool, error) {
	servingKey := RedisContentEdgeLocationTable + infra.URLToSafeName(serverID) + RedisContentEdgeLocationServingAttr
	result, err := r.rdb.SIsMember(r.ctx, servingKey, cid).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check if content(%s) is served by server(%s): %w", cid, serverID, err)
	}
	return result, nil
}

// ContentServerList returns the list of servers currently serving a content ID
func (r *RedisMicroserviceConfiguration) ContentServerList(cid string) ([]string, error) {
	locationKey := RedisContentMetadataTable + infra.URLToSafeName(cid) + RedisContentMetadataLocationAttr
	servers, err := r.rdb.SMembers(r.ctx, locationKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get server list for content(%s): %w", cid, err)
	}
	return servers, nil
}

// IsContentBeingServed returns whether or not a piece of content is being served anywhere on the network
func (r *RedisMicroserviceConfiguration) IsContentBeingServed(cid string) (bool, error) {
	servers, err := r.ContentServerList(cid)
	if err != nil {
		return false, fmt.Errorf("failed to check if content(%s) is being served: %w", cid, err)
	}
	return len(servers) > 0, nil
}

// WasContentPulled returns whether or not a content was pulled by the network(as opposed to manually pushed to the network)
func (r *RedisMicroserviceConfiguration) WasContentPulled(cid string, serverID string) (bool, error) {
	mechanismKey := RedisContentServeMechanismTable + infra.URLToSafeName(cid) +
		infra.URLToSafeName(serverID) + RedisContentServeMechanismPulledAttr

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
func (r *RedisMicroserviceConfiguration) CreateContentPullRule(rule string) error {
	if err := r.rdb.SAdd(r.ctx, RedisContentPullRulesList, rule).Err(); err != nil {
		return fmt.Errorf("failed to add rule(%s) to rule list: %w", rule, err)
	}
	return nil
}

// DeleteContentPullRule removes a pull rule from the store
func (r *RedisMicroserviceConfiguration) DeleteContentPullRule(rule string) error {
	if err := r.rdb.SRem(r.ctx, RedisContentPullRulesList, rule).Err(); err != nil {
		return fmt.Errorf("failed to remove rule(%s) from rule list: %w", rule, err)
	}
	return nil
}

// GetContentPullRules returns all content pull rules currently in effect
func (r *RedisMicroserviceConfiguration) GetContentPullRules() ([]string, error) {
	rules, err := r.rdb.SMembers(r.ctx, RedisContentPullRulesList).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve pull rules: %w", err)
	}
	return rules, nil
}

// ContentPullRuleExists checks if a pull rule is currently in effect
func (r *RedisMicroserviceConfiguration) ContentPullRuleExists(rule string) (bool, error) {
	result, err := r.rdb.SIsMember(r.ctx, RedisContentPullRulesList, rule).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check if rule(%s) exists: %w", rule, err)
	}
	return result, nil
}
