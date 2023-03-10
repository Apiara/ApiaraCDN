package crow

import (
	"fmt"
	"sort"
	"sync"
)

/*
EvenDataAllocator implements DataAllocator using a strategy that first
prioritizes allocating larger pieces of content over smaller ones, then
prioritizes allocating each piece of content an even amount of times
*/
type EvenDataAllocator struct {
	mutex        *sync.Mutex
	dataClasses  []int64
	dataClassMap map[string]int
	dataQueues   []*dataPriorityQueue
}

// used for sorting int64 slices
type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

/*
NewEvenDataAllocator returns an EvenDataAllocator. sizeClasses should be a sorted
list of int64 with the first element being 0
*/
func NewEvenDataAllocator(sizeClasses []int64) *EvenDataAllocator {
	// Ensure sizeClasses is in ascending order with element 0 being 0
	sort.Sort(int64arr(sizeClasses))
	if sizeClasses[0] != 0 {
		sizeClasses = append([]int64{0}, sizeClasses...)
	}

	// Create size class priority queues
	dataQueues := make([]*dataPriorityQueue, len(sizeClasses))
	for i := 0; i < len(sizeClasses); i++ {
		dataQueues[i] = newDataPriorityQueue()
	}

	return &EvenDataAllocator{
		mutex:        &sync.Mutex{},
		dataClasses:  sizeClasses,
		dataClassMap: make(map[string]int),
		dataQueues:   dataQueues,
	}
}

/*
binary search modification returning closest data class to availablesSpace.
If ceil = true then it returns the closest data class > availableSpace,
otherwise it returns the closest data class < availableSpace
*/
func approximateBinarySearch(list []int64, key int64, ceil bool) int {
	low := 0
	mid := 0
	high := len(list) - 1
	for low <= high {
		mid = (high + low) / 2
		if key > list[mid] {
			low = mid + 1
		} else if key < list[mid] {
			high = mid - 1
		} else {
			return mid
		}
	}

	if ceil && high != len(list) {
		return high + 1
	}
	return high
}

// NewEntry adds a piece of content with 'size' to be allocated
func (d *EvenDataAllocator) NewEntry(id string, size int64) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, ok := d.dataClassMap[id]; ok {
		return fmt.Errorf("failed to add entry. Entry with name %s already exists", id)
	}

	classIdx := approximateBinarySearch(d.dataClasses, size, true)
	pq := d.dataQueues[classIdx]
	pq.push(id, &dataItem{
		index:       -1,
		allocations: 0,
		byteSize:    size,
		id:          id,
	})

	d.dataClassMap[id] = classIdx
	return nil
}

// DelEntry removes a piece of content from the allocator
func (d *EvenDataAllocator) DelEntry(id string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, ok := d.dataClassMap[id]; !ok {
		return fmt.Errorf("failed to delete entry, Entry with name %s doesn't exist", id)
	}

	classIdx := d.dataClassMap[id]
	pq := d.dataQueues[classIdx]
	pq.remove(id)

	delete(d.dataClassMap, id)
	return nil
}

/*
AllocateSpace returns a list of content ids for the requesting endpoint
with 'availableSpace' space to download and serve. It attempts to optimize
session length while ensuring each piece of content is allocated to an
equal amount of endpoints
*/
func (d *EvenDataAllocator) AllocateSpace(availableSpace int64) ([]string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	allocations := make([]string, 0)
	classIdx := approximateBinarySearch(d.dataClasses, availableSpace, false)

	for classIdx != 0 && availableSpace > 0 {
		// Retrieve class resources
		nextClass := d.dataClasses[classIdx-1]
		classQueue := d.dataQueues[classIdx]

		// Get all possible allocations from class
		popped := []*dataItem{}
		item := classQueue.pop()
		if item != nil {
			popped = append(popped, item)
		}
		for item != nil && availableSpace > nextClass {
			allocations = append(allocations, item.id)
			availableSpace -= item.byteSize
			item.allocations++

			item = classQueue.pop()
			if item != nil {
				popped = append(popped, item)
			}
		}

		// Push items allocated back into PQ with updated priorities
		for _, item = range popped {
			classQueue.push(item.id, item)
		}

		// Skip to next size class that can be served
		classIdx--
		for classIdx != 0 && availableSpace < d.dataClasses[classIdx-1] {
			classIdx--
		}
	}

	return allocations, nil
}