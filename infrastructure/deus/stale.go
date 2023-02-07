package deus

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/cyprus"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
DataValidator represents an object that can check if a content ID
has gone stale or not.
*/
type DataValidator interface {
	IsStale(cid string) (bool, error)
}

/*
ChecksumDataValidator implements DataValidator by checking if the checksum
for the provided content id matches the internal checksum for the content
*/
type ChecksumDataValidator struct {
	accessor          internalDataAccessor
	mediaPreprocessor cyprus.DataPreprocessor
	dataIndex         state.ContentMetadataStateReader
	contentBaseURL    string
}

/*
NewChecksumDataValidator creates a new ChecksumDataValidator with the provided
preprocessor and dataIndex, retrieving internal copies of data from the
internalDataAddr base URL
*/
func NewChecksumDataValidator(internalDataAddr string, preprocessor cyprus.DataPreprocessor,
	dataIndex state.ContentMetadataStateReader) (*ChecksumDataValidator, error) {

	contentBaseURL, err := url.JoinPath(internalDataAddr, infra.CryptDataStorageDir)
	if err != nil {
		return nil, fmt.Errorf("Failed to create content download base URL: %w", err)
	}
	metadataBaseURL, err := url.JoinPath(internalDataAddr, infra.CompleteMediaMapDir)
	if err != nil {
		return nil, fmt.Errorf("Failed to create metadata download base URL: %w", err)
	}
	keyBaseURL, err := url.JoinPath(internalDataAddr, infra.AESKeyStorageDir)
	if err != nil {
		return nil, fmt.Errorf("Failed to create key download base URL: %w", err)
	}

	return &ChecksumDataValidator{
		accessor: &aesInternalDataAccessor{
			metadataBaseURL: metadataBaseURL,
			keyBaseURL:      keyBaseURL,
			contentBaseURL:  contentBaseURL,
			retrieveFile:    cyprus.DownloadFile,
		},
		mediaPreprocessor: preprocessor,
		dataIndex:         dataIndex,
		contentBaseURL:    contentBaseURL,
	}, nil
}

func (c *ChecksumDataValidator) getRawMediaInternalChecksum(cid string) ([]byte, error) {
	// Calculate content location
	fid, err := c.dataIndex.GetContentFunctionalID(cid)
	if err != nil {
		return nil, fmt.Errorf("Failed to retrieve %s functional id for stale detection: %w", cid, err)
	}
	dataURL, err := url.JoinPath(c.contentBaseURL, fid)
	if err != nil {
		return nil, fmt.Errorf("Failed to create %s data download URL: %w", cid, err)
	}

	// Retrieve content key
	cryptKey, err := c.accessor.GetKey(cid)
	if err != nil {
		return nil, err
	}

	// Calculate content checksum
	hasher := sha256.New()
	if err = c.accessor.GetContent(dataURL, cryptKey, hasher); err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func (c *ChecksumDataValidator) getVODMediaInternalChecksum(cid string) ([]byte, error) {
	// Download VOD manifest
	var mediaMap cyprus.VODManifest
	if err := c.accessor.GetMetadata(cid, &mediaMap); err != nil {
		return nil, err
	}

	// Retrieve content key
	cryptKey, err := c.accessor.GetKey(cid)
	if err != nil {
		return nil, err
	}

	// Get list of all functional IDs in increasing order for reproducability
	functionalIDs := make([]string, 0)
	for _, stream := range mediaMap.Streams {
		for _, segment := range stream.Segments {
			functionalIDs = append(functionalIDs, segment.FunctionalID)
		}
	}
	sort.Strings(functionalIDs)

	// Get content and calculate checksum
	hasher := sha256.New()
	for _, fid := range functionalIDs {
		downloadURL, err := url.JoinPath(c.contentBaseURL, fid)
		if err != nil {
			return nil, fmt.Errorf("Failed to create %s download URL for %s checksum creation: %w", fid, cid, err)
		}
		if err = c.accessor.GetContent(downloadURL, cryptKey, hasher); err != nil {
			return nil, err
		}
	}
	return hasher.Sum(nil), nil
}

func (c *ChecksumDataValidator) getRawMediaExternalChecksum(ingest cyprus.MediaIngest) ([]byte, error) {
	// Read metadata
	metadata := ingest.Result.(*cyprus.RawMedia)
	file, err := os.Open(metadata.File)
	if err != nil {
		return nil, fmt.Errorf("Failed to open %s ingest file: %w", metadata.URL, err)
	}
	defer file.Close()

	// Calculate checksum
	hasher := sha256.New()
	if _, err = io.Copy(hasher, file); err != nil {
		return nil, fmt.Errorf("Failed to hash %s sample: %w", metadata.URL, err)
	}
	return hasher.Sum(nil), nil
}

func (c *ChecksumDataValidator) getVODMediaExternalChecksum(ingest cyprus.MediaIngest) ([]byte, error) {
	// Download media from internet, lookup relevant key, encrypt, calculate checksum
	mediaMap := ingest.Result.(*cyprus.VODManifest)

	// Get list of all functional IDs in increasing order for reproducability
	fileMap := make(map[string]string)
	functionalIDs := make([]string, 0)
	for _, stream := range mediaMap.Streams {
		for _, segment := range stream.Segments {
			functionalIDs = append(functionalIDs, segment.FunctionalID)
			fileMap[segment.FunctionalID] = segment.File
		}
	}
	sort.Strings(functionalIDs)

	// Calculate checksums
	hasher := sha256.New()
	for _, fid := range functionalIDs {
		file, err := os.Open(fileMap[fid])
		if err != nil {
			return nil, fmt.Errorf("Failed to open %s ingest file: %w", fid, err)
		}
		if _, err = io.Copy(hasher, file); err != nil {
			return nil, fmt.Errorf("Failed to hash %s sample for %s checksum: %w", fid, mediaMap.URL, err)
		}
	}
	return hasher.Sum(nil), nil
}

/*
IsStale checks if a piece of content that is being served by the network
is representative of the contents current state on the rest of the internet.
If not, the data is considered stale and the function returns true.
*/
func (c *ChecksumDataValidator) IsStale(cid string) (bool, error) {
	// Ingest content from external source
	ingest, err := c.mediaPreprocessor.IngestMedia(cid)
	if err != nil {
		return false, fmt.Errorf("Failed to ingest %s: %w", cid, err)
	}
	defer cyprus.RemoveIngestArtifacts(ingest)

	// Calculate checksums for both internal and external data samples
	var internalChecksum []byte
	var externalChecksum []byte
	switch ingest.Type {
	case cyprus.RawMediaType:
		internalChecksum, err = c.getRawMediaInternalChecksum(cid)
		if err != nil {
			return false, fmt.Errorf("Failed to get %s internal checksum: %w", cid, err)
		}
		externalChecksum, err = c.getRawMediaExternalChecksum(ingest)
		if err != nil {
			return false, fmt.Errorf("Failed to get %s external checksum: %w", cid, err)
		}
		break
	case cyprus.VODMediaType:
		internalChecksum, err = c.getVODMediaInternalChecksum(cid)
		if err != nil {
			return false, fmt.Errorf("Failed to get %s internal checksum: %w", cid, err)
		}
		externalChecksum, err = c.getVODMediaExternalChecksum(ingest)
		if err != nil {
			return false, fmt.Errorf("Failed to get %s external checksum: %w", cid, err)
		}
	}

	// Compare data samples
	return !bytes.Equal(internalChecksum, externalChecksum), nil
}
