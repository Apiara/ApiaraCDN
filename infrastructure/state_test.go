package infrastructure

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataIndex(t *testing.T) {
	redisAddr := ":7777"
	state := NewRedisDataIndex(redisAddr)

	cid := "http://www.random.com/something"
	fid := "functionalID"
	size := int64(1024)
	resources := []string{"random", "random2", "random3"}
	resourceMap := make(map[string]bool)
	for _, resource := range resources {
		resourceMap[resource] = true
	}

	if err := state.Create(cid, fid, size, resources); err != nil {
		t.Fatalf("Failed to create entry: %v", err)
	}
	defer state.Delete(cid)

	foundResources, err := state.GetResources(cid)
	if err != nil {
		t.Fatalf("Failed to get resources: %v", err)
	}
	for _, found := range foundResources {
		if _, ok := resourceMap[found]; !ok {
			t.Fatalf("Failed to return valid resource. Got %s", found)
		}
	}

	foundFid, err := state.GetFunctionalID(cid)
	if err != nil {
		t.Fatalf("Failed to get functional id: %v", err)
	}
	assert.Equal(t, foundFid, fid, "Functional IDs not equal")

	foundCid, err := state.GetContentID(fid)
	if err != nil {
		t.Fatalf("Failed to get content id: %v", err)
	}
	assert.Equal(t, foundCid, cid, "Content IDs not equal")

	foundSize, err := state.GetSize(cid)
	if err != nil {
		t.Fatalf("Failed to get content size: %v", err)
	}
	assert.Equal(t, foundSize, size, "Sizes are not equal")

	if err = state.Delete(cid); err != nil {
		t.Fatalf("Failed to delete entry: %v", err)
	}
}
