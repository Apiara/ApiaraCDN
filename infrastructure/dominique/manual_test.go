package dominique

import (
	"fmt"
	"testing"
	"time"
)

func TestPostgresRemediationQueue(t *testing.T) {
	// Create resources
	dbName := "dominique_test"
	user := "postgres"
	port := 5432
	host := "localhost"
	password := "dominique_test"

	clientRep := &ClientReport{
		SessionID:    fmt.Sprintf("session_id_%d", time.Now().Unix()),
		FunctionalID: "fid",
		ContentID:    "cid",
		IP:           "ip",
		BytesRecv:    1024,
		BytesNeeded:  1025,
	}
	endpointRep := &EndpointReport{
		SessionID:    clientRep.SessionID,
		FunctionalID: clientRep.FunctionalID,
		ContentID:    clientRep.ContentID,
		IP:           "ip",
		Identity:     "eid",
		BytesServed:  1024,
	}

	// Test write
	queue, err := NewPostgresRemediationQueue(host, port, user, password, dbName)
	if err != nil {
		t.Fatalf("Failed to create postgres queue: %v", err)
	}
	if err = queue.Write(clientRep, endpointRep); err != nil {
		t.Fatalf("Failed to write to postgres queue: %v", err)
	}
}
