package crow

import (
	"encoding/gob"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/stretchr/testify/assert"
)

func TestFindNearestDataClass(t *testing.T) {
	classes := []int64{0, 1024, 4096, 65549, 328748}

	availableSpace := int64(1071)
	classIdx := approximateBinarySearch(classes, availableSpace, true)
	assert.Equal(t, 2, classIdx, "Got wrong class index: %d", classIdx)

	classIdx = approximateBinarySearch(classes, availableSpace, false)
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
	allocator := NewCompoundLocationDataAllocator(classes, func(string) (DataAllocator, error) {
		return NewEvenDataAllocator(classes), nil
	})

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

func TestPrecompDataAllocator(t *testing.T) {
	// Test create entry
	content := []string{"cid1", "cid2", "cid3"}
	sizes := []int64{786, 3997, 78112}
	edgeServerPort := ":7891"
	edgeServerAddr := "http://127.0.0.1" + edgeServerPort
	updateFreq := time.Second
	classes := []int64{5096, 1024, 65549, 328748}

	// Start test key server
	go func() {
		api := http.NewServeMux()
		api.HandleFunc(infra.DamoclesServiceAPIPriorityListResource,
			func(resp http.ResponseWriter, req *http.Request) {
				returnMap := make(map[string]int64)
				for i, id := range content {
					returnMap[id] = int64(i)
				}
				if err := gob.NewEncoder(resp).Encode(returnMap); err != nil {
					resp.WriteHeader(http.StatusInternalServerError)
				}
			})
		http.ListenAndServe(edgeServerPort, api)
	}()

	// Create resources
	allocator, err := NewPrecomputedDataAllocator(edgeServerAddr, updateFreq, classes)
	if err != nil {
		assert.Nil(t, err, "should not return error")
	}
	for i := 0; i < len(content); i++ {
		if err := allocator.NewEntry(content[i], sizes[i]); err != nil {
			t.Fatalf("failed to create new entry %s: %v", content[i], err)
		}
	}

	// Test allocation
	time.Sleep(updateFreq * 2)
	availableSpace := int64(7600)
	expectedAllocations := []string{"cid1", "cid2"}
	ids, err := allocator.AllocateSpace(availableSpace)
	if err != nil {
		t.Fatalf("Failed to get allocations: %v", err)
	}
	sort.Strings(ids)

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
