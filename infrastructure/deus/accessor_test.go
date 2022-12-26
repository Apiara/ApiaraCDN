package deus

import (
	"bytes"
	"testing"

	"github.com/Apiara/ApiaraCDN/infrastructure/cyprus"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestAESInternalDataAccessor(t *testing.T) {
	accessor := &aesInternalDataAccessor{
		metadataBaseURL: "./test_resources/metadata",
		keyBaseURL:      "./test_resources/keys",
		contentBaseURL:  "./test_resources/content",
		retrieveFile:    cyprus.CopyFromDisk,
	}

	// Test GetMetadata
	actualContentMap := cyprus.VODManifest{
		URL:          "test_manifest_url",
		FunctionalID: "test_fid",
		Streams: []cyprus.VODStream{
			{
				URL:          "test_stream_1_url",
				FunctionalID: "test_stream_1_fid",
				Segments: []cyprus.VODSegment{
					{
						Index:        0,
						URL:          "test_stream_1_segment_1_url",
						FunctionalID: "test_stream_1_segment_1_fid",
						Checksum:     "test_stream_1_segment_1_checksum",
					},
				},
			},
		},
	}

	metadataPath := accessor.metadataBaseURL + "/test_cmap.json"
	var contentMap cyprus.VODManifest
	if err := accessor.GetMetadata(metadataPath, &contentMap); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, true, cmp.Equal(actualContentMap, contentMap), "Metadata read was not correct")

	// Test GetKey
	keyPattern := []byte{0xde, 0xad, 0xbe, 0xef}
	actualKey := make([]byte, 32)
	for i := 0; i < len(actualKey); {
		for _, hexByte := range keyPattern {
			actualKey[i] = hexByte
			i++
		}
	}

	keyPath := accessor.keyBaseURL + "/test_key"
	cryptKey, err := accessor.GetKey(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, actualKey, cryptKey, "Read key is not the same as expected key")

	// Test GetContent with decryption
	contentURL := accessor.contentBaseURL + "/test_content_crypt"
	var buf bytes.Buffer
	if err = accessor.GetContent(contentURL, cryptKey, &buf); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "secret", string(buf.Bytes()), "Got wrong key")
}
