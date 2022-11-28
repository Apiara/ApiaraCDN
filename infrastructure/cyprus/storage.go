package cyprus

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

const (
	publishedFilePerms = 0644
)

/*
StorageManager represents an object that can publish the output of a MediaDigest
to the data stores for use by client and endpoint resource retrieval APIs
*/
type StorageManager interface {
	Publish(digest MediaDigest) error
	PurgeByURL(url string) error
	PurgeByFunctionalID(fid string) error
}

/*
FilesystemStorageManager implements StorageManager and uses filesystem
directories to store the processed data
*/
type FilesystemStorageManager struct {
	state          infra.DataIndex
	keyDir         string
	dataDir        string
	partialMapDir  string
	completeMapDir string
}

/*
NewFilesystemStorageManager creates a new FilesystemStorageManager where all
data is stored in subdirectories of storageDir and indexed via state
*/
func NewFilesystemStorageManager(storageDir string, state infra.DataIndex) (*FilesystemStorageManager, error) {
	dirs := []string{
		path.Join(storageDir, infra.AESKeyStorageDir),
		path.Join(storageDir, infra.CryptDataStorageDir),
		path.Join(storageDir, infra.PartialMapDir),
		path.Join(storageDir, infra.CompleteMediaMapDir),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("Failed to make dir %s: %w", dir, err)
		}
	}

	return &FilesystemStorageManager{
		state:          state,
		keyDir:         dirs[0],
		dataDir:        dirs[1],
		partialMapDir:  dirs[2],
		completeMapDir: dirs[3],
	}, nil
}

// publishManifest publishes digested manifest resources to the appropriate data stores
func (s *FilesystemStorageManager) publishManifest(mediaMap manifest, key []byte) ([]string, error) {
	// Create list of all created resources
	resources := make([]string, 0)

	// Publish symmetric key
	urlFname := infra.URLToSafeName(mediaMap.URL)
	keyFname := path.Join(s.keyDir, urlFname)
	if err := ioutil.WriteFile(keyFname, key, publishedFilePerms); err != nil {
		return resources, err
	}
	resources = append(resources, keyFname)

	// Publish encrypted segments
	var err error
	for _, mediaStream := range mediaMap.Streams {
		for _, mediaSegment := range mediaStream.Segments {
			dataFname := path.Join(s.dataDir, mediaSegment.FunctionalID)
			if err = os.Rename(mediaSegment.File, dataFname); err != nil {
				return resources, err
			}
			resources = append(resources, dataFname)
		}
	}

	// Publish complete map
	completeMapFname := path.Join(s.completeMapDir, urlFname)
	serialCompleteMap, err := json.Marshal(mediaMap)
	if err != nil {
		return resources, err
	}
	if err = ioutil.WriteFile(completeMapFname, serialCompleteMap, publishedFilePerms); err != nil {
		return resources, err
	}
	resources = append(resources, completeMapFname)

	// Publish partial map
	partialMapFname := path.Join(s.partialMapDir, mediaMap.FunctionalID)
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
func (s *FilesystemStorageManager) publishRawMedia(media rawMedia, key []byte) ([]string, error) {
	// Create list of all created resources
	resources := make([]string, 0)

	// Publish symmetric key
	urlFname := infra.URLToSafeName(media.URL)
	keyFname := path.Join(s.keyDir, urlFname)
	if err := ioutil.WriteFile(keyFname, key, publishedFilePerms); err != nil {
		return resources, err
	}
	resources = append(resources, keyFname)

	// Publish encrypted media file
	dataFname := path.Join(s.dataDir, media.FunctionalID)
	if err := os.Rename(media.File, dataFname); err != nil {
		return resources, err
	}
	resources = append(resources, dataFname)

	// Publish complete media definition
	mediaDefFname := path.Join(s.completeMapDir, urlFname)
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
	pMediaDefFname := path.Join(s.partialMapDir, media.FunctionalID)
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
func (s *FilesystemStorageManager) purgeFiles(resources []string) {
	for _, resource := range resources {
		if err := os.Remove(resource); err != nil {
			log.Println(err)
		}
	}
}

// Publish publishes the output of a MediaDigest to the appropriate datastores
func (s *FilesystemStorageManager) Publish(digest MediaDigest) error {
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
		resources, err = s.publishRawMedia(media, digest.CryptKey)
		break
	case VODMedia:
		mediaManifest := digest.Result.(manifest)
		url = mediaManifest.URL
		fid = mediaManifest.FunctionalID
		resources, err = s.publishManifest(mediaManifest, digest.CryptKey)
		break
	default:
		return fmt.Errorf("Failed to publish. MediaType %d does not exist", digest.Type)
	}

	// Purge all created resources if anything failed
	if err != nil {
		s.purgeFiles(resources)
		return err
	}

	// Publish state update to state index
	if err = s.state.Create(url, fid, digest.ByteSize, resources); err != nil {
		return err
	}
	return nil
}

/*
purge removes all filesystem resources created for a URL/FID
as well as deletes all associated indexed information
*/
func (s *FilesystemStorageManager) purge(url string) error {
	// Purge resource files
	resources, err := s.state.GetResources(url)
	if err != nil {
		return fmt.Errorf("Failed to read resource list for URL %s: %w", url, err)
	}
	s.purgeFiles(resources)
	return s.state.Delete(url)
}

// PurgeByURL allows purging by URL key
func (s *FilesystemStorageManager) PurgeByURL(url string) error {
	return s.purge(url)
}

// PurgeByFunctionalID allows purging by functional ID
func (s *FilesystemStorageManager) PurgeByFunctionalID(fid string) error {
	// Get URL
	url, err := s.state.GetContentID(fid)
	if err != nil {
		return err
	}
	return s.purge(url)
}
