package damocles

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNeedEndpointAllocator(t *testing.T) {
	connStore := &mockConnectionManager{}
	tracker := &mockNeedTracker{}

	fid := "fid1"
	data, _ := json.Marshal(&allocateRequest{
		Serving: []string{fid, "test"},
	})
	endpoint := &mockWebsocket{msgs: [][]byte{data}}

	allocator := NewNeedEndpointAllocator(connStore, tracker)
	if err := allocator.PlaceEndpoint(endpoint); err != nil {
		t.Fatalf("Failed to place endpoint: %v", err)
	}

	assert.Equal(t, tracker.allocates, 1, "Failed to track allocation")
	assert.Equal(t, endpoint.writeCount, 1, "Failed to write response to endpoint")
}
