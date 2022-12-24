package cyprus

import (
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func copyFromDisk(fname string, outFile io.Writer) error {
	inFile, err := os.Open(fname)
	if err != nil {
		return err
	}

	if _, err = io.Copy(outFile, inFile); err != nil {
		return err
	}
	return nil
}

func TestRawPreprocessor(t *testing.T) {
	preprocessor := &RawPreprocessor{
		outputDir:    "./test_resources/working",
		retrieveFile: copyFromDisk,
	}

	testFname := "./test_resources/hls/index_1_1.ts"
	ingest, err := preprocessor.IngestMedia(testFname)
	if err != nil {
		t.Fatalf("Failed to ingest media file %s: %v", testFname, err)
	}

	if ingest.Type != RawMediaType {
		t.Fatalf("Failed to tag the media correctly as %d. Tagged as %d instead", RawMediaType, ingest.Type)
	}

	media, ok := ingest.Result.(RawMedia)
	if !ok {
		t.Fatalf("Failed to return proper type rawMedia")
	}
	defer os.Remove(media.File)

	if media.URL != testFname {
		t.Fatalf("Failed to return proper URL name %s. Got %s instead", testFname, media.URL)
	}

	if !strings.HasPrefix(path.Base(media.File), "ingest_") {
		t.Fatalf("Failed to return proper outfile pattern %s. Got name %s instead", ingestFilePattern, media.File)
	}
}

func TestHLSPreprocessor(t *testing.T) {
	preprocessor := &HLSPreprocessor{
		outputDir:    "./test_resources/working",
		retrieveFile: copyFromDisk,
	}

	testFname := "./test_resources/hls/master.m3u8"
	ingest, err := preprocessor.IngestMedia(testFname)
	if err != nil {
		t.Fatalf("Failed to ingest media file %s: %v", testFname, err)
	}

	assert.Equal(t, ingest.Type, VODMediaType, "Ingest tag incorrect")

	mediaManifest, ok := ingest.Result.(VODManifest)
	if !ok {
		t.Fatalf("Failed to return proper type manifest")
	}

	assert.Equal(t, mediaManifest.URL, testFname, "Wrong stored URL")
	assert.Equal(t, len(mediaManifest.Streams), 2, "Wrong number of parsed streams")

	// Cleanup
	for _, mediaStream := range mediaManifest.Streams {
		for _, mediaSegment := range mediaStream.Segments {
			os.Remove(mediaSegment.File)
		}
	}
}
