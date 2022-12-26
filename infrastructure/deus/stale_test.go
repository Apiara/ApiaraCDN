package deus

import (
	"os"
	"testing"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/cyprus"
	"github.com/stretchr/testify/assert"
)

func TestChecksumDataValidator(t *testing.T) {
	// Create resources
	testFid := "test_content_crypt"
	originalFile := "./test_resources/content/orig_test_content"
	testCid := "./test_resources/content/test_content"
	if err := os.Link(originalFile, testCid); err != nil {
		t.Fatal(err)
	}

	preprocessor := &cyprus.MockDataPreprocessor{
		Ingests: map[string]cyprus.MediaIngest{
			testCid: {
				Type: cyprus.RawMediaType,
				Result: &cyprus.RawMedia{
					File: testCid,
				},
			},
		},
	}
	dataIndex := infra.NewMockDataIndex()
	dataIndex.Create(testCid, testFid, 22, []string{})

	validator := &ChecksumDataValidator{
		accessor: &aesInternalDataAccessor{
			metadataBaseURL: "./test_resources/metadata",
			keyBaseURL:      "./test_resources/keys",
			contentBaseURL:  "./test_resources/content",
			retrieveFile:    cyprus.CopyFromDisk,
		},
		mediaPreprocessor: preprocessor,
		dataIndex:         dataIndex,
		contentBaseURL:    "./test_resources/content",
	}

	// Test RawMedia stale check
	isStale, err := validator.IsStale(testCid)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, false, isStale, "Got wrong stale result")
}
