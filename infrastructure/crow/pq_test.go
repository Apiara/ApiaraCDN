package crow

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataPriorityQueue(t *testing.T) {
	pq := newDataPriorityQueue()

	items := []*dataItem{
		{
			allocations: 20,
			id:          "2",
		},
		{
			allocations: 17,
			id:          "1",
		},
		{
			allocations: 47,
			id:          "3",
		},
		{
			allocations: 1,
			id:          "0",
		},
	}

	for _, item := range items {
		pq.push(item.id, item)
	}

	for i := 0; i < len(items); i++ {
		item := pq.pop()
		assert.Equal(t, item.id, strconv.Itoa(i), "Wrong PQ pop order")
	}

	item := pq.pop()
	assert.Nil(t, item, "Should have popped nil")

	pq.push(items[0].id, items[0])

	if err := pq.remove(items[0].id); err != nil {
		t.Fatal("Failed to remove item")
	}
	if err := pq.remove(items[0].id); err == nil {
		t.Fatal("Should have failed to remove item")
	}
}
