package deus

import (
  "testing"
  "net/http"
  "encoding/json"
  infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

func TestMasterContentManager(t *testing.T) {
  // Start test server
  mockPort := ":11111"
  mockAPIAddr := "http://localhost" + mockPort

  go func(){
    functionalID := "functional"
    mockServer := http.NewServeMux()
    mockServer.HandleFunc("/process", func(resp http.ResponseWriter, req *http.Request) {})
    mockServer.HandleFunc("/status", func(resp http.ResponseWriter, req *http.Request) {
      json.NewEncoder(resp).Encode(&infra.StatusResponse{"complete", &functionalID})
    })
    mockServer.HandleFunc("/delete", func(resp http.ResponseWriter, req *http.Request) {})
    mockServer.HandleFunc("/publish", func(resp http.ResponseWriter, req *http.Request) {})
    mockServer.HandleFunc("/purge", func(resp http.ResponseWriter, req *http.Request) {})
    http.ListenAndServe(mockPort, mockServer)
  }()


  // Create resources
  cid := "cid"
  serverAddr := "server"
  state := &MockContentState{make(map[string]struct{})}
  manager := NewMasterContentManager(state, mockAPIAddr, mockAPIAddr)

  // Do Serve test
  if err := manager.Serve(cid, serverAddr, true); err != nil {
    t.Fatalf("Failed to start serving data: %v", err)
  }

  if serving, _ := state.IsBeingServed(cid, serverAddr); !serving {
    t.Fatalf("Failed propogate serve success to content state")
  }

  // Do Remove test
  if err := manager.Remove(cid, serverAddr, true); err != nil {
    t.Fatalf("Failed to stop serving data: %v", err)
  }

  if serving, _ := state.IsBeingServed(cid, serverAddr); serving {
    t.Fatalf("Failed propogate serve removal to content state")
  }

}
