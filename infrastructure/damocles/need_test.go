package damocles

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDesperationTracker(t *testing.T) {
	timeSlice := time.Second / 2
	category := "fid"
	tracker := NewDesperationTracker(timeSlice)

	if err := tracker.CreateCategory(category); err != nil {
		t.Fatalf("Failed to create category %s: %v", category, err)
	}

	requests := 10
	allocations := 5
	for i := 0; i < requests; i++ {
		if err := tracker.AddRequest(category); err != nil {
			t.Fatalf("Failed to add request to %s: %v", category, err)
		}
	}
	for i := 0; i < allocations; i++ {
		if err := tracker.AddAllocation(category); err != nil {
			t.Fatalf("Failed to add allocation to %s: %v", category, err)
		}
	}

	score, err := tracker.GetScore(category)
	if err != nil {
		t.Fatalf("Failed to get score for %s: %v", category, err)
	}

	expectedScore := int64(requests - allocations)
	assert.Equal(t, score, expectedScore, "Failed to get correct score")

	time.Sleep(timeSlice * 2)
	score, err = tracker.GetScore(category)
	if err != nil {
		t.Fatalf("Failed to get score for %s: %v", category, err)
	}
	assert.Equal(t, score, int64(0), "Failed to clear scores")

	if err = tracker.DelCategory(category); err != nil {
		t.Fatalf("Failed to delete category %s: %v", category, err)
	}
}
