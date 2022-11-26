package deus

import (
  "testing"
  "time"
)

func TestThresholdPullDecider(t *testing.T) {
  validator := &mockContentValidator{}
  manager := &mockContentManager{make(map[string]bool)}
  state := &mockContentState{map[string]struct{}{}}
  threshold := 10
  interval := time.Second

  // Test start serving
  cid := "cid"
  server := "server"
  decider := NewThresholdPullDecider(validator, manager, state, threshold, interval)
  for i := 0; i < threshold + 1; i++ {
    if err := decider.NewRequest(cid, server); err != nil {
      t.Fatalf("Failed to log new request: %v", err)
    }
  }

  time.Sleep(interval + interval/2)
  if _, ok := manager.serving[cid + server]; !ok {
    t.Fatal("Failed to start serving content when passed threshold")
  }
  state.serveSet[cid + server] = struct{}{}

  // Test stop serving
  time.Sleep(interval)
  if _, ok := manager.serving[cid + server]; ok {
    t.Fatal("Failed to stop serving content when dipped below threshold")
  }

}
