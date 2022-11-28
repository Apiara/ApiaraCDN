package cyprus

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/etherlabsio/go-m3u8/m3u8"
)

// MediaType tags the type of data a preprocessor returns
type MediaType int

const (
	// Represents raw media files(ex. MP4, MOV)
	RawMedia MediaType = iota

	// Represents manifest based VOD formats(ex. HLS, MPEG-DASH)
	VODMedia
)

const (
	ingestFilePattern = "ingest_*"
)

// MediaIngest is the return type for a DataPreprocessor
type MediaIngest struct {
	Type   MediaType
	Result interface{}
}

/*
	DataPreprocessor represents an object that can perform preprocessing tasks

for data attempting to be uploaded to the network
*/
type DataPreprocessor interface {
	IngestMedia(url string) (MediaIngest, error) //Outputs folder output and error
}

/*
	CompoundPreprocessor implements DataPreprocessor by checking URL extensions

and routing to the media type specific preprocessor
*/
type CompoundPreprocessor struct {
	extensionMap map[string]DataPreprocessor
}

/*
	NewCompoundPreprocessor creates a new CompoundPreprocessor with the

provided extension to DataPreprocessor mapping
*/
func NewCompoundPreprocessor(extensionMap map[string]DataPreprocessor) *CompoundPreprocessor {
	return &CompoundPreprocessor{
		extensionMap: extensionMap,
	}
}

// IngestMedia routes to the correct preprocessor and delegates the IngestMedia call
func (c *CompoundPreprocessor) IngestMedia(url string) (MediaIngest, error) {
	ext := filepath.Ext(strings.TrimSpace(url))
	preprocessor, ok := c.extensionMap[ext]
	if !ok {
		return MediaIngest{}, fmt.Errorf("Failed to find proper preprocessor for %s", url)
	}
	return preprocessor.IngestMedia(url)
}

// RawPreprocessor implements DataPreprocessor for raw media files(ex. mp4)
type RawPreprocessor struct {
	outputDir    string
	retrieveFile func(*os.File, string) error
}

func NewRawPreprocessor(workingPath string) *RawPreprocessor {
	return &RawPreprocessor{
		outputDir:    workingPath,
		retrieveFile: downloadFile,
	}
}

// IngestMedia returns the filepath of the downloaded raw media file
func (r *RawPreprocessor) IngestMedia(fileURL string) (MediaIngest, error) {
	// Download single media file
	outFile, err := ioutil.TempFile(r.outputDir, ingestFilePattern)
	if err != nil {
		return MediaIngest{}, fmt.Errorf("Failed to create ingest file: %w", err)
	}
	defer outFile.Close()

	if err := r.retrieveFile(outFile, fileURL); err != nil {
		return MediaIngest{}, fmt.Errorf("Failed to download %s to %s: %w", fileURL, outFile.Name(), err)
	}
	return MediaIngest{
		Type: RawMedia,
		Result: rawMedia{
			URL:  fileURL,
			File: outFile.Name(),
		},
	}, nil
}

// HLSPreprocessor implements DataPreprocessor for HLS Manifest Files
type HLSPreprocessor struct {
	outputDir    string
	retrieveFile func(*os.File, string) error
}

// NewHLSPreprocessor creates a new HLSPreprocessor where outputs are stored at workingDir
func NewHLSPreprocessor(workingDir string) *HLSPreprocessor {
	return &HLSPreprocessor{
		outputDir:    workingDir,
		retrieveFile: downloadFile,
	}
}

func (r *HLSPreprocessor) parseStreamPlaylist(basePath string, playlist *m3u8.Playlist) (stream, error) {
	hlsSegments := playlist.Segments()
	genericSegments := make([]segment, 0, len(hlsSegments))
	for i, hlsSegment := range hlsSegments {
		segmentURL := path.Join(basePath, hlsSegment.Segment)
		segmentFile, err := ioutil.TempFile(r.outputDir, ingestFilePattern)
		if err != nil {
			return stream{}, fmt.Errorf("Failed to create ingest file: %w", err)
		}

		if err := r.retrieveFile(segmentFile, segmentURL); err != nil {
			segmentFile.Close()
			return stream{}, fmt.Errorf("Failed to download segment %s: %w", segmentURL, err)
		}
		segmentFile.Close()

		genericSegments = append(genericSegments, segment{
			Index:        i,
			URL:          segmentURL,
			FunctionalID: "",
			File:         segmentFile.Name(),
		})
	}

	return stream{
		FunctionalID: "",
		Segments:     genericSegments,
	}, nil
}

func (r *HLSPreprocessor) getManifest(manifestURL string) (*m3u8.Playlist, error) {
	// Download manifest file
	outFile, err := ioutil.TempFile("", "tmp_manifest_*.m3u8")
	if err != nil {
		return nil, fmt.Errorf("Failed to create temporary file: %w", err)
	}
	if err = r.retrieveFile(outFile, manifestURL); err != nil {
		outFile.Close()
		return nil, fmt.Errorf("Failed to download manifest at %s: %w", manifestURL, err)
	}
	outFile.Close()

	// Parse manifest file
	playlist, err := m3u8.ReadFile(outFile.Name())
	if err != nil {
		return nil, fmt.Errorf("Failed to parse .m3u8 manifest %s: %w", outFile.Name(), err)
	}
	os.Remove(outFile.Name())

	return playlist, err
}

/*
	Ingest fetches all data associated with manifestURL and creates an internal

manifest object to represent the VOD media map and point to appropriate system file locations
*/
func (r *HLSPreprocessor) IngestMedia(manifestURL string) (MediaIngest, error) {
	// Fetch and parse master manifest
	masterManifest, err := r.getManifest(manifestURL)
	if err != nil {
		return MediaIngest{}, fmt.Errorf("Failed to download manifest %s: %w", manifestURL, err)
	}

	baseURL := path.Dir(manifestURL)
	streams := make([]stream, 0)
	if masterManifest.IsMaster() { // Handle case of master manifest with different stream sub manifests
		playlists := masterManifest.Playlists()
		for _, playlist := range playlists {
			// Retrieve and parse sub manifests
			subManifestURL := path.Join(baseURL, playlist.URI)
			subManifest, err := r.getManifest(subManifestURL)
			if err != nil {
				return MediaIngest{}, fmt.Errorf("Failed to retrieve sub manifest %s: %w", subManifestURL, err)
			}

			// generate internal 'stream' object based on sub manifest
			mediaStream, err := r.parseStreamPlaylist(path.Dir(subManifestURL), subManifest)
			if err != nil {
				return MediaIngest{}, fmt.Errorf("Failed to parse sub manifest %s: %w", subManifestURL, err)
			}

			// complete and store processed stream
			mediaStream.URL = subManifestURL
			streams = append(streams, mediaStream)
		}
	} else { // Handle case of single manifest with no sub streams
		// generate internal 'stream' object for single manifest
		mediaStream, err := r.parseStreamPlaylist(baseURL, masterManifest)
		if err != nil {
			return MediaIngest{}, fmt.Errorf("Failed to parse manifest %s: %w", manifestURL, err)
		}

		// store stream under default URL name to indicate no sub manifests
		mediaStream.URL = DefaultStreamName
		streams = append(streams, mediaStream)
	}

	// Create and return preprocess result
	return MediaIngest{
		Type: VODMedia,
		Result: manifest{
			URL:          manifestURL,
			FunctionalID: "",
			Streams:      streams,
		},
	}, nil
}
