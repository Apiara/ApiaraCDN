package amada

import (
	"testing"
)

func TestRedisGeoServerIndex(t *testing.T) {
	region := "Oregon"
	server := "server"
	redisAddr := ":7777"
	servers := NewRedisGeoServerIndex(redisAddr)

	// Test Set
	if err := servers.SetRegionAddress(region, server); err != nil {
		t.Fatalf("Failed to set region address: %v\n", err)
	}

	// Test Get
	retAddress, err := servers.GetAddress(region)
	if err != nil {
		t.Fatalf("Failed to get region address: %v\n", err)
	}
	if retAddress != server {
		t.Fatalf("Failed to get correct region address. Got %s instead\n", retAddress)
	}

	// Test Remove
	if err = servers.RemoveRegionAddress(region); err != nil {
		t.Fatalf("Failed to remove region address: %v\n", err)
	}
	retAddress, err = servers.GetAddress(region)
	if err == nil {
		t.Fatalf("Found region address when should have failed: %s\n", retAddress)
	}
}
