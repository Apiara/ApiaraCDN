package cyprus

import (
  "path"
  "fmt"
  "os"
  "io/ioutil"
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

// MediaMetadata is the return type for a DataPreprocessor
type MediaMetadata struct {
  Type MediaType
  Result interface{}
}

/* DataPreprocessor represents an object that can perform preprocessing tasks
for data attempting to be uploaded to the network */
type DataPreprocessor interface {
  CreateMediaResources(url string) (MediaMetadata, error) //Outputs folder output and error
}

// RawDataPreprocessor implements DataPreprocessor for raw media files(ex. mp4)
type RawDataPreprocessor struct {
  outputDir string
  generateUniqueID func(string) string
}

// CreateMediaResources returns the filepath of the downloaded raw media file
func (r *RawDataPreprocessor) CreateMediaResources(fileURL string) (MediaMetadata, error) {
  // Download single media file
  mediaFName := path.Join(r.outputDir, r.generateUniqueID(fileURL))
  outFile, err := os.Create(mediaFName)
  if err != nil {
    return MediaMetadata{}, fmt.Errorf("Failed to create file %s: %w", mediaFName, err)
  }

  if err := downloadFile(outFile, fileURL); err != nil {
    return MediaMetadata{}, fmt.Errorf("Failed to download %s to %s: %w", fileURL, mediaFName, err)
  }
  return MediaMetadata{
    Type: RawMedia,
    Result: mediaFName,
  }, nil
}

// HLSDataPreprocessor implements DataPreprocessor for HLS Manifest Files
type HLSDataPreprocessor struct {
  outputDir string
  generateUniqueID func(string) string
}

func (r *HLSDataPreprocessor) parseStreamPlaylist(basePath string, playlist *m3u8.Playlist) (stream, error) {
  hlsSegments := playlist.Segments()
  genericSegments := make([]segment, 0, len(hlsSegments))
  for i, hlsSegment := range hlsSegments {
    segmentURL := path.Join(basePath, hlsSegment.Segment)
    systemFName := path.Join(r.outputDir, r.generateUniqueID(segmentURL))

    segmentFile, err := os.Create(systemFName)
    if err != nil {
      return stream{}, fmt.Errorf("Failed to create system file %s: %w", systemFName, err)
    }
    if err := downloadFile(segmentFile, segmentURL); err != nil {
      return stream{}, fmt.Errorf("Failed to download segment %s: %w", segmentURL, err)
    }

    genericSegments = append(genericSegments, segment{
      Index: i,
      URL: segmentURL,
      FunctionalID: "",
      File: systemFName,
    })
  }

  return stream{
    FunctionalID: "",
    Segments: genericSegments,
  }, nil
}

func (r *HLSDataPreprocessor) getManifest(manifestURL string) (*m3u8.Playlist, error) {
  // Download manifest file
  outFile, err := ioutil.TempFile("", "tmp_manifest_*.m3u8")
  if err != nil {
    return nil, fmt.Errorf("Failed to create temporary file: %w", err)
  }
  if err = downloadFile(outFile, manifestURL); err != nil {
    return nil, fmt.Errorf("Failed to download manifest at %s: %w", manifestURL, err)
  }

  // Parse manifest file
  playlist, err := m3u8.ReadFile(outFile.Name())
  if err != nil {
    return nil, fmt.Errorf("Failed to parse .m3u8 manifest %s: %w", outFile.Name(), err)
  }
  os.Remove(outFile.Name())

  return playlist, err
}

/* CreateMediaResources fetches all data associated with manifestURL and creates an internal
manifest object to represent the VOD media map and point to appropriate system file locations */
func (r *HLSDataPreprocessor) CreateMediaResources(manifestURL string) (MediaMetadata, error) {
  // Fetch and parse master manifest
  masterManifest, err := r.getManifest(manifestURL)
  if err != nil {
    return MediaMetadata{}, fmt.Errorf("Failed to download manifest %s: %w", manifestURL, err)
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
        return MediaMetadata{}, fmt.Errorf("Failed to retrieve sub manifest %s: %w", subManifestURL, err)
      }

      // generate internal 'stream' object based on sub manifest
      mediaStream, err := r.parseStreamPlaylist(path.Dir(subManifestURL), subManifest)
      if err != nil {
        return MediaMetadata{}, fmt.Errorf("Failed to parse sub manifest %s: %w", subManifestURL, err)
      }

      // complete and store processed stream
      mediaStream.URL = subManifestURL
      streams = append(streams, mediaStream)
    }
  } else { // Handle case of single manifest with no sub streams
    // generate internal 'stream' object for single manifest
    mediaStream, err := r.parseStreamPlaylist(baseURL, masterManifest)
    if err != nil {
      return MediaMetadata{}, fmt.Errorf("Failed to parse manifest %s: %w", manifestURL, err)
    }

    // store stream under default URL name to indicate no sub manifests
    mediaStream.URL = DefaultStreamName
    streams = append(streams, mediaStream)
  }

  // Create and return preprocess result
  return MediaMetadata{
    Type: VODMedia,
    Result: manifest{
      URL: manifestURL,
      FunctionalID: "",
      Streams: streams,
    },
  }, nil
}
