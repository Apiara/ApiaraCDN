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
    File string `json:"filename"`
  }
)

// helper func to download files from the internet
func downloadFile(outFile *os.File, url string) error {
  defer outFile.Close()

  resp, err := http.Get(url)
  if err != nil {
    return err
  }
  defer resp.Body.Close()

  _, err = io.Copy(outFile, resp.Body)
  return err
}
