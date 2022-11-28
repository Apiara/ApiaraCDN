package crow

import (
	"container/heap"
	"fmt"
	"math"
)

type dataItem struct {
	index       int
	allocations int64
	byteSize    int64
	id          string
}

/*
implements a simple minimum priority queue prioritizing
data that has the least number of allocations
*/
type dataPriorityQueue struct {
	pq        *minDataPQ
	updateMap map[string]*dataItem
}

// creates a new dataPriorityQueue
func newDataPriorityQueue() *dataPriorityQueue {
	pq := make(minDataPQ, 0)
	return &dataPriorityQueue{
		pq:        &pq,
		updateMap: make(map[string]*dataItem),
	}
}

func (d *dataPriorityQueue) push(id string, item *dataItem) {
	d.updateMap[id] = item
	heap.Push(d.pq, item)
}

func (d *dataPriorityQueue) pop() *dataItem {
	if d.pq.Len() == 0 {
		return nil
	}

	item := heap.Pop(d.pq).(*dataItem)
	delete(d.updateMap, item.id)
	return item
}

func (d *dataPriorityQueue) remove(id string) error {
	// Fetch item to remove
	item, ok := d.updateMap[id]
	if !ok {
		return fmt.Errorf("Failed to remove %s. %s doesn't exist", id, id)
	}

	// Force item to top of heap and pop
	d.pq.updatePriority(item, math.MinInt64)
	heap.Pop(d.pq)

	delete(d.updateMap, item.id)
	return nil
}

// Implements heap.Interface for use as minimum priority queue
type minDataPQ []*dataItem

func (pq minDataPQ) Len() int { return len(pq) }

func (pq minDataPQ) Less(i, j int) bool {
	return pq[i].allocations < pq[j].allocations
}

func (pq minDataPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *minDataPQ) Push(x any) {
	n := len(*pq)
	item := x.(*dataItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *minDataPQ) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*pq = old[:n-1]
	return item
}

func (pq *minDataPQ) updatePriority(item *dataItem, allocations int64) {
	item.allocations = allocations
	heap.Fix(pq, item.index)
}
