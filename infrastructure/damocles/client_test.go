package damocles

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNeedClientServicer(t *testing.T) {
	connStore := &mockConnectionManager{}
	tracker := &mockNeedTracker{0, 0}
	servicer := NewNeedClientServicer(connStore, tracker)

	fid := "fid"
	data, _ := json.Marshal(&clientRequest{fid})
	client := &mockWebsocket{msgs: [][]byte{data}}

	if err := servicer.MatchAndSignal(client); err != nil {
		t.Fatalf("Failed to MatchAndSignal: %v", err)
	}
	assert.Equal(t, client.writeCount, 1, "Got wrong number of client writes: ", strconv.Itoa(client.writeCount))
}
