package deus

import (
	"encoding/json"
	"net/http"
	"testing"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

func TestMasterContentManager(t *testing.T) {
	// Start test server
	mockPort := ":11111"
	mockAPIAddr := "http://localhost" + mockPort

	cid := "cid"
	functionalID := "functional"

	go func() {
		mockServer := http.NewServeMux()
		mockServer.HandleFunc("/process", func(resp http.ResponseWriter, req *http.Request) {})
		mockServer.HandleFunc("/status", func(resp http.ResponseWriter, req *http.Request) {
			md := infra.PostProcessingMetadata{FunctionalID: functionalID, ByteSize: 0}
			json.NewEncoder(resp).Encode(&infra.StatusResponse{Status: infra.FinishedProcessing, Metadata: &md})
		})
		mockServer.HandleFunc("/delete", func(resp http.ResponseWriter, req *http.Request) {})
		mockServer.HandleFunc("/publish", func(resp http.ResponseWriter, req *http.Request) {})
		mockServer.HandleFunc("/purge", func(resp http.ResponseWriter, req *http.Request) {})
		mockServer.HandleFunc("/category/add", func(resp http.ResponseWriter, req *http.Request) {})
		mockServer.HandleFunc("/category/del", func(resp http.ResponseWriter, req *http.Request) {})
		http.ListenAndServe(mockPort, mockServer)
	}()

	// Create resources
	serverAddr := mockAPIAddr
	microserviceState := state.NewMockMicroserviceState()
	manager, err := NewMasterContentManager(microserviceState, microserviceState, mockAPIAddr, mockAPIAddr)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Do Serve test
	if err := manager.Serve(cid, serverAddr, true); err != nil {
		t.Fatalf("Failed to start serving data: %v", err)
	}

	if serving, _ := microserviceState.IsContentServedByServer(cid, serverAddr); !serving {
		t.Fatalf("Failed propogate serve success to content state")
	}

	// Update data index as it is supposed to be updated by process
	microserviceState.CreateContentEntry(cid, functionalID, 1024, []string{})

	// Do Remove test
	if err := manager.Remove(cid, serverAddr, true); err != nil {
		t.Fatalf("Failed to stop serving data: %v", err)
	}

	if serving, _ := microserviceState.IsContentServedByServer(cid, serverAddr); serving {
		t.Fatalf("Failed propogate serve removal to content state")
	}
}
