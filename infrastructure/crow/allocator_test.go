package crow

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindNearestDataClass(t *testing.T) {
	classes := []int64{0, 1024, 4096, 65549, 328748}
	allocator := NewEvenDataAllocator(classes)

	availableSpace := int64(1071)
	classIdx := allocator.findNearestDataClass(availableSpace, true)
	assert.Equal(t, 2, classIdx, "Got wrong class index: %d", classIdx)

	classIdx = allocator.findNearestDataClass(availableSpace, false)
	assert.Equal(t, 1, classIdx, "Got wrong class index: %d", classIdx)
}

func TestEvenDataAllocator(t *testing.T) {
	classes := []int64{4096, 1024, 65549, 328748}
	allocator := NewEvenDataAllocator(classes)

	// Test create entry
	content := []string{"cid1", "cid2", "cid3"}
	sizes := []int64{786, 3997, 78112}
	for i := 0; i < len(content); i++ {
		if err := allocator.NewEntry(content[i], sizes[i]); err != nil {
			t.Fatalf("Failed to create new entry %s: %v", content[i], err)
		}
	}

	// Test allocation
	availableSpace := int64(7600)
	expectedAllocations := []string{"cid2", "cid1"}
	ids, err := allocator.AllocateSpace(availableSpace)
	if err != nil {
		t.Fatalf("Failed to get allocations: %v", err)
	}
	fmt.Println(ids)

	assert.Equal(t, len(expectedAllocations), len(ids), "Wrong amount of allocations returned")
	for i := 0; i < len(ids); i++ {
		assert.Equal(t, expectedAllocations[i], ids[i], "Wrong ID returned")
	}

	// Test remove entry
	err = allocator.DelEntry("cid1")
	if err != nil {
		t.Fatalf("Failed to delete entry")
	}
}

func TestCompoundLocationDataAllocator(t *testing.T) {
	classes := []int64{4096, 1024, 65549, 328748}
	allocator := NewCompoundLocationDataAllocator(classes)

	// Test underlying resource tracking and creation
	sizes := []int64{4000, 60000, 200}
	content := []string{"cid1", "cid2", "cid3"}
	locations := []string{"loc1", "loc2", "loc3"}
	for _, loc := range locations {
		for i, cid := range content {
			assert.Nil(t, allocator.NewEntry(loc, cid, sizes[i]), "expected no error")
		}
	}

	for _, loc := range locations {
		size, ok := allocator.entryCount[loc]
		assert.Equal(t, true, ok, "expected resource map to be created")
		assert.Equal(t, len(content), size, "expected content count to be equal to amount added")
	}

	// Test underlying resource deletion
	for _, loc := range locations {
		for _, cid := range content {
			assert.Nil(t, allocator.DelEntry(loc, cid), "expected no error")
		}
	}

	for _, loc := range locations {
		_, ok := allocator.entryCount[loc]
		assert.Equal(t, false, ok, "expected resource map to be delete")
	}
}
