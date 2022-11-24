package cyprus

import (
  "os"
  "net/http"
  "io"
)

const (
  DefaultStreamName = "default"
)

type (
  manifest struct {
    URL string `json:"url"`
    FunctionalID string `json:"fid"`
    Streams []stream `json:"streams"`
  }

  stream struct {
    URL string `json:"url"`
    FunctionalID string `json:"fid"`
    Segments []segment `json:"segments"`
  }

  segment struct {
    Index int `json:"index"`
    URL string `json:"url"`
    FunctionalID string `json:"fid"`
    Checksum string `json:"checksum"`
    File string `json:"-"`
  }
)

type (
  partialManifest struct {
    FunctionalID string `json:"fid"`
    Segments []partialSegment `json:"segments"`
  }

  partialSegment struct {
    FunctionalID string `json:"fid"`
    Checksum string `json:"checksum"`
  }
)

func completeToPartialManifest(mediaMap manifest) partialManifest {
  pMediaMap := partialManifest{
    FunctionalID: mediaMap.FunctionalID,
    Segments: make([]partialSegment, 0),
  }

  for _, mediaStream := range mediaMap.Streams {
    for _, mediaSegment := range mediaStream.Segments {
      pMediaMap.Segments = append(pMediaMap.Segments, partialSegment{
        FunctionalID: mediaSegment.FunctionalID,
        Checksum: mediaSegment.Checksum,
      })
    }
  }
  return pMediaMap
}

type rawMedia struct {
  URL string `json:"url"`
  FunctionalID string `json:"fid"`
  Checksum string `json:"url"`
  File string `json:"-"`
}

type partialRawMedia struct {
  FunctionalID string `json:"fid"`
  Checksum string `json:"url"`
}

func completeToPartialRawMedia(media rawMedia) partialRawMedia {
  return partialRawMedia{
    FunctionalID: media.FunctionalID,
    Checksum: media.Checksum,
  }
}

// helper func to download files from the internet
func downloadFile(outFile *os.File, url string) error {
  resp, err := http.Get(url)
  if err != nil {
    return err
  }
  defer resp.Body.Close()

  _, err = io.Copy(outFile, resp.Body)
  return err
}
