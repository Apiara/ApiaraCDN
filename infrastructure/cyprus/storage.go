package cyprus

import (
  "os"
  "encoding/json"
  "encoding/hex"
  "context"
  "github.com/go-redis/redis/v8"
  "crypto/sha256"
  "path"
  "io/ioutil"
  "fmt"
  "log"
)

const (
  publishedFilePerms = 0644
)

const (
  // Key mapping FIDs to Plaintext URLs
  redisFunctionalToURLKey = "cyprus:functional:"

  /* Key mapping Safe URLs to FIDs. Note URLs must be encoded in a safe format
  that doesn't include special characters, specifically ":" */
  redisURLToFunctionalKey = "cyprus:url:"

  // Key mapping Safe URLs to a set of all filesystem resources created under URL
  redisURLToResourcesKey = "cyprus:resources:"
)

/* StorageManager represents an object that can publish the output of a MediaDigest
to the data stores for use by client and endpoint resource retrieval APIs */
type StorageManager interface {
  Publish(digest MediaDigest) error
  PurgeByURL(url string) error
  PurgeByFID(fid string) error
}

/* RedisStorageManager implements StorageManager and uses Redis
to store state of published resources */
type RedisStorageManager struct {
  rdb *redis.Client
  ctx context.Context
  keyDir string
  dataDir string
  partialMapDir string
  completeMapDir string
}

/* Converts URL with possible unsafe characters
to a unique hex string 24 bytes long */
func urlToSafeName(url string) string {
  sum := sha256.Sum224([]byte(url))
  safe := hex.EncodeToString(sum[:])
  return safe
}

// publishManifest publishes digested manifest resources to the appropriate data stores
func (r *RedisStorageManager) publishManifest(mediaMap manifest, key []byte) ([]string, error) {
  // Create list of all created resources
  resources := make([]string, 0)

  // Publish symmetric key
  urlFname := urlToSafeName(mediaMap.URL)
  keyFname := path.Join(r.keyDir, urlFname)
  if err := ioutil.WriteFile(keyFname, key, publishedFilePerms); err != nil {
    return resources, err
  }
  resources = append(resources, keyFname)

  // Publish encrypted segments
  var err error
  for _, mediaStream := range mediaMap.Streams {
    for _, mediaSegment := range mediaStream.Segments {
      dataFname := path.Join(r.dataDir, mediaSegment.FunctionalID)
      if err = os.Rename(mediaSegment.File, dataFname); err != nil {
        return resources, err
      }
      resources = append(resources, dataFname)
    }
  }

  // Publish complete map
  completeMapFname := path.Join(r.completeMapDir, urlFname)
  serialCompleteMap, err := json.Marshal(mediaMap)
  if err != nil {
    return resources, err
  }
  if err = ioutil.WriteFile(completeMapFname, serialCompleteMap, publishedFilePerms); err != nil {
    return resources, err
  }
  resources = append(resources, completeMapFname)

  // Publish partial map
  partialMapFname := path.Join(r.completeMapDir, mediaMap.FunctionalID)
  partialMap := completeToPartialManifest(mediaMap)
  serialPartialMap, err := json.Marshal(partialMap)
  if err != nil {
    return resources, err
  }
  if err = ioutil.WriteFile(partialMapFname, serialPartialMap, publishedFilePerms); err != nil {
    return resources, err
  }
  resources = append(resources, partialMapFname)

  return resources, nil
}

// publishRawMedia publishes the digested media data to the proper data stores
func (r *RedisStorageManager) publishRawMedia(media rawMedia, key []byte) ([]string, error) {
  // Create list of all created resources
  resources := make([]string, 0)

  // Publish symmetric key
  urlFname := urlToSafeName(media.URL)
  keyFname := path.Join(r.keyDir, urlFname)
  if err := ioutil.WriteFile(keyFname, key, publishedFilePerms); err != nil {
    return resources, err
  }
  resources = append(resources, keyFname)

  // Publish encrypted media file
  dataFname := path.Join(r.dataDir, media.FunctionalID)
  if err := os.Rename(media.File, dataFname); err != nil {
    return resources, err
  }
  resources = append(resources, dataFname)

  // Publish complete media definition
  mediaDefFname := path.Join(r.completeMapDir, urlFname)
  serialMediaDef, err := json.Marshal(media)
  if err != nil {
    return resources, err
  }
  if err = ioutil.WriteFile(mediaDefFname, serialMediaDef, publishedFilePerms); err != nil {
    return resources, err
  }
  resources = append(resources, mediaDefFname)

  // Publish partial media definition
  pMedia := completeToPartialRawMedia(media)
  pMediaDefFname := path.Join(r.partialMapDir, media.FunctionalID)
  serialPMedia, err := json.Marshal(pMedia)
  if err != nil {
    return resources, err
  }
  if err = ioutil.WriteFile(pMediaDefFname, serialPMedia, publishedFilePerms); err != nil {
    return resources, err
  }
  resources = append(resources, pMediaDefFname)

  return resources, nil
}

// purgeFiles deletes all files specified in 'resources'
func (r *RedisStorageManager) purgeFiles(resources []string) {
  for _, resource := range resources {
    if err := os.Remove(resource); err != nil {
      log.Println(err)
    }
  }
}

/* indexCreatedResources updates the redis cache with created
filesystem resources and key mappings for the URL/FID keys */
func (r *RedisStorageManager) indexCreatedResources(url string, fid string, resources []string) error {
  // Create FunctionalID to URL mapping
  fidMapKey := redisFunctionalToURLKey + ":" + fid
  err := r.rdb.Set(r.ctx, fidMapKey, url, 0).Err()
  if err != nil {
    return fmt.Errorf("Failed to add functional id to url mapping: %w", err)
  }

  // Create URL to FunctionalID mapping
  urlKey := urlToSafeName(url)
  urlMapKey := redisURLToFunctionalKey + ":" + urlKey
  if err := r.rdb.Set(r.ctx, urlMapKey, fid, 0).Err(); err != nil {
    return fmt.Errorf("Failed to add url to functional id mapping: %w", err)
  }

  // Create URL to Resources mapping
  resourceMapKey := redisURLToResourcesKey + ":" + urlKey
  for _, resource := range resources {
    val, err := r.rdb.SAdd(r.ctx, resourceMapKey, resource).Result()
    if err != nil {
      return fmt.Errorf("Failed to add url to resources mapping: %w", err)
    } else if val != 1 {
      return fmt.Errorf("Failed to add url to resources mapping")
    }
  }
  return nil
}

// Publish publishes the output of a MediaDigest to the appropriate datastores
func (r *RedisStorageManager) Publish(digest MediaDigest) error {
  var err error
  var url string
  var fid string
  var resources []string

  // Publish digest based on MediaType
  switch digest.Type {
  case RawMedia:
    media := digest.Result.(rawMedia)
    url = media.URL
    fid = media.FunctionalID
    resources, err = r.publishRawMedia(media, digest.CryptKey)
    break
  case VODMedia:
    mediaManifest := digest.Result.(manifest)
    url = mediaManifest.URL
    fid = mediaManifest.FunctionalID
    resources, err = r.publishManifest(mediaManifest, digest.CryptKey)
    break
  default:
    return fmt.Errorf("Failed to publish. MediaType %d does not exist", digest.Type)
  }

  // Purge all created resources if anything failed
  if err != nil {
    r.purgeFiles(resources)
    return err
  }

  // Publish state update to redis database
  if err = r.indexCreatedResources(url, fid, resources); err != nil {
    return err
  }
  return nil
}

/* purge removes all filesystem resources created for a URL/FID
as well as deletes all associated redis state entries */
func (r *RedisStorageManager) purge(url string, fid string) error {
  // Purge resource files
  urlKey := urlToSafeName(url)
  resourceMapKey := redisURLToResourcesKey + ":" + urlKey
  resources, err := r.rdb.SMembers(r.ctx, resourceMapKey).Result()
  if err == redis.Nil {
    return fmt.Errorf("No resources found under URL %s", url)
  } else if err != nil {
    return fmt.Errorf("Failed to read resource list for URL %s: %w", url, err)
  }
  r.purgeFiles(resources)

  // Delete resource list from index
  if err := r.rdb.Del(r.ctx, resourceMapKey).Err(); err != nil {
    return fmt.Errorf("Failed to remove resource mappings: %w", err)
  }

  // Delete key mappings
  urlMapKey := redisURLToFunctionalKey + ":" + urlKey
  fidMapKey := redisFunctionalToURLKey + ":" + fid

  if err = r.rdb.Del(r.ctx, urlMapKey).Err(); err != nil {
    return fmt.Errorf("Failed to remove url to functional ID mapping: %w", err)
  }
  if err = r.rdb.Del(r.ctx, fidMapKey).Err(); err != nil {
    return fmt.Errorf("Failed to remove functional ID to url mapping: %w", err)
  }
  return nil
}

// PurgeByURL allows purging by URL key
func (r *RedisStorageManager) PurgeByURL(url string) error {
  // Get Functional ID
  urlKey := urlToSafeName(url)
  urlMapKey := redisURLToFunctionalKey + ":" + urlKey
  fid, err := r.rdb.Get(r.ctx, urlMapKey).Result()
  if err != nil {
    return fmt.Errorf("Failed to get URL to Functional ID key: %w", err)
  }

  return r.purge(url, fid)
}

// PurgeByFunctionalID allows purging by functional ID
func (r *RedisStorageManager) PurgeByFunctionalID(fid string) error {
  // Get URL
  fidMapKey := redisFunctionalToURLKey + ":" + fid
  url, err := r.rdb.Get(r.ctx, fidMapKey).Result()
  if err != nil {
    return fmt.Errorf("Failed to get URL to Functional ID key: %w", err)
  }

  return r.purge(url, fid)
}
