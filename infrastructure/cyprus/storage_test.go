package cyprus

import (
	"os"
	"testing"

	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

func TestFilesystemStorageManager(t *testing.T) {
	// Test Locations
	testFile, err := createTestIngestFile("./test_resources/hls/index_1_1.ts")
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	storageDir := "./test_resources/working/"

	// Cleanup
	defer func() {
		os.RemoveAll("./test_resources/working/")
		os.MkdirAll("./test_resources/working/", 0777)
	}()

	// Test
	state := state.NewMockMicroserviceState()
	storage, err := NewFilesystemStorageManager(storageDir, state)
	if err != nil {
		t.Fatalf("Failed to create redis storage manager: %v", err)
	}

	url := "http://www.randomsite.com/what?cid=hello"
	fid := "functional1"
	cryptKey, err := generateRandomBytes(DefaultAESKeySize)
	if err != nil {
		t.Fatalf("Failed to generate test key: %v", err)
	}

	digest := MediaDigest{
		Type:         VODMediaType,
		CryptKey:     cryptKey,
		FunctionalID: fid,
		Result: VODManifest{
			URL:          url,
			FunctionalID: fid,
			Streams: []VODStream{
				{
					URL:          "http://stream.com",
					FunctionalID: "functional2",
					Segments: []VODSegment{
						{
							Index:        0,
							URL:          "http://segment.com",
							FunctionalID: "functional3",
							Checksum:     "checksum",
							File:         testFile,
						},
					},
				},
			},
		},
	}

	// Publish
	if err = storage.Publish(digest); err != nil {
		t.Fatalf("Failed to publish manifest: %v", err)
	}

	// PurgeByURL
	if err = storage.PurgeByURL(url); err != nil {
		t.Fatalf("Failed to purge by url: %v", err)
	}

	// Publish
	testFile, err = createTestIngestFile("./test_resources/hls/index_1_1.ts")
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	digest.Result.(VODManifest).Streams[0].Segments[0].File = testFile

	if err = storage.Publish(digest); err != nil {
		t.Fatalf("Failed to publish manifest: %v", err)
	}

	// PurgeByFunctionalID
	if err = storage.PurgeByFunctionalID(fid); err != nil {
		t.Fatalf("Failed to purge by fid: %v", err)
	}
}
