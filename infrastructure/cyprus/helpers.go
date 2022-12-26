package cyprus

import (
	"io"
	"net/http"
	"os"
)

const (
	DefaultStreamName = "default"
)

type (
	VODManifest struct {
		URL          string      `json:"url"`
		FunctionalID string      `json:"fid"`
		Streams      []VODStream `json:"streams"`
	}

	VODStream struct {
		URL          string       `json:"url"`
		FunctionalID string       `json:"fid"`
		Segments     []VODSegment `json:"segments"`
	}

	VODSegment struct {
		Index        int    `json:"index"`
		URL          string `json:"url"`
		FunctionalID string `json:"fid"`
		Checksum     string `json:"checksum"`
		File         string `json:"-"`
	}
)

type (
	PartialVODManifest struct {
		FunctionalID string              `json:"fid"`
		Segments     []PartialVODSegment `json:"segments"`
	}

	PartialVODSegment struct {
		FunctionalID string `json:"fid"`
		Checksum     string `json:"checksum"`
	}
)

func completeToPartialManifest(mediaMap VODManifest) PartialVODManifest {
	pMediaMap := PartialVODManifest{
		FunctionalID: mediaMap.FunctionalID,
		Segments:     make([]PartialVODSegment, 0),
	}

	for _, mediaStream := range mediaMap.Streams {
		for _, mediaSegment := range mediaStream.Segments {
			pMediaMap.Segments = append(pMediaMap.Segments, PartialVODSegment{
				FunctionalID: mediaSegment.FunctionalID,
				Checksum:     mediaSegment.Checksum,
			})
		}
	}
	return pMediaMap
}

type RawMedia struct {
	URL          string `json:"url"`
	FunctionalID string `json:"fid"`
	Checksum     string `json:"checksum"`
	File         string `json:"-"`
}

type PartialRawMedia struct {
	FunctionalID string `json:"fid"`
	Checksum     string `json:"checksum"`
}

func completeToPartialRawMedia(media RawMedia) PartialRawMedia {
	return PartialRawMedia{
		FunctionalID: media.FunctionalID,
		Checksum:     media.Checksum,
	}
}

// helper func to download files from the internet
func DownloadFile(url string, outFile io.Writer) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(outFile, resp.Body)
	return err
}

// Testing replacement function for DownloadFile
func CopyFromDisk(fname string, outFile io.Writer) error {
	inFile, err := os.Open(fname)
	if err != nil {
		return err
	}

	if _, err = io.Copy(outFile, inFile); err != nil {
		return err
	}
	return nil
}
