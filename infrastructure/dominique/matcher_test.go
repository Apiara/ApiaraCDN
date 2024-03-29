package dominique

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimedSessionProcessor(t *testing.T) {
	// Create resources
	ReportCollectionFrequency = time.Second
	timeout := time.Second
	timeseries := &mockTimeseriesDB{make(map[string][]Report), make(map[string][]SessionDescription)}
	matcher := NewTimedSessionProcessor(timeout, timeseries)

	// Test good match
	sessionID := "session1"
	fid := "fid1"
	bytesRecv := int64(1024)
	cReport := &ClientReport{
		SessionID:    sessionID,
		FunctionalID: fid,
		IP:           "testip",
		BytesRecv:    bytesRecv,
		BytesNeeded:  bytesRecv,
	}
	eReport := &EndpointReport{
		SessionID:    sessionID,
		FunctionalID: fid,
		IP:           "testip",
		Identity:     "id",
		BytesServed:  bytesRecv,
	}

	if err := matcher.AddReport(cReport); err != nil {
		t.Fatalf("Failed to add client report: %v", err)
	}
	if err := matcher.AddReport(eReport); err != nil {
		t.Fatalf("Failed to add endpoint report: %v", err)
	}

	assert.Equal(t, len(timeseries.reports[sessionID]), 0, "Should have received no reports")
	assert.Equal(t, len(timeseries.descs[sessionID]), 1, "Should have received 1 description")

	// Test timeout match
	if err := matcher.AddReport(cReport); err != nil {
		t.Fatalf("Failed to add client report: %v", err)
	}

	time.Sleep(timeout * 2)
	assert.Equal(t, len(timeseries.reports[sessionID]), 1, "Should have received 1 report")
}
