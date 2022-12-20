package dominique

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRemediationProcessing(t *testing.T) {
	// Create resources
	byteOffset := int64(1024)
	remediators := []Remediator{
		NewTimeframeRemediator(),
		NewByteOffsetRemediator(byteOffset),
	}

	clientRep := &ClientReport{
		SessionID:    "sid",
		FunctionalID: "fid",
		ContentID:    "cid",
		IP:           "ip",
		BytesRecv:    2048,
		BytesNeeded:  3000,
	}
	endpointRep := &EndpointReport{
		SessionID:    clientRep.SessionID,
		FunctionalID: clientRep.FunctionalID,
		ContentID:    clientRep.ContentID,
		IP:           "ip",
		Identity:     "eid",
		BytesServed:  1025,
	}
	timeseries := &mockTimeseriesDB{
		map[string][]Report{
			clientRep.SessionID: {clientRep, endpointRep},
		},
		make(map[string][]SessionDescription),
	}
	handleQueue := &mockManualRemediationQueue{}

	// Test processSession where ByteOffsetRemediator should succeed
	err := processSession(clientRep.SessionID, time.Time{}, time.Time{},
		timeseries, remediators, handleQueue)

	if err != nil {
		t.Fatalf("Failed to successfully process session: %v", err)
	}
	assert.Equal(t, 1, len(timeseries.descs[clientRep.SessionID]), "Failed to reconcile reports")
}
