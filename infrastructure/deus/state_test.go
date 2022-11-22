package deus

import (
  "testing"
)

func TestRedisContentState(t *testing.T) {
  redisAddr := ":7777"
  state := NewRedisContentState(redisAddr)

  cid := "content"
  server := "server"

  // Test set
  if err := state.Set(cid, server, true); err != nil {
    t.Fatalf("Failed to set content serving state: %v\n", err)
  }

  // Test serve check
  serving, err := state.IsBeingServed(cid, server)
  if err != nil {
    t.Fatalf("Failed to check if content being served: %v\n", err)
  }
  if !serving {
    t.Fatalf("Failed to see that content is being served\n")
  }

  // Test dynamic status check
  dyn, err := state.WasDynamicallySet(cid, server)
  if err != nil {
    t.Fatalf("Failed to check if content was dynamically set: %v\n", err)
  }
  if !dyn {
    t.Fatalf("Failed to see that content was set dynamically\n")
  }

  // Test removal
  if err = state.Remove(cid, server); err != nil {
    t.Fatalf("Failed to remove content serve state: %v\n", err)
  }
  serving, err = state.IsBeingServed(cid, server)
  if err != nil {
    t.Fatalf("Failed to check if content being served: %v\n", err)
  }
  if serving {
    t.Fatalf("Failed to see that content is not being served\n")
  }
}
