package crow

import (
	"fmt"
	"sync"
)

/*
LocationAwareDataAllocator represents an object that can allocate
content to endpoints based on location and space availability
*/
type LocationAwareDataAllocator interface {
	NewEntry(loc string, cid string, size int64) error
	DelEntry(loc string, cid string) error
	AllocateSpace(loc string, availableSpace int64) ([]string, error)
}

/*
CompoundLocationDataAllocator implements LocationAwareDataAllocator
in a simple way by encapsulating multiple DataAllocators
*/
type CompoundLocationDataAllocator struct {
	createAllocator func() DataAllocator
	mutex           *sync.Mutex
	locations       map[string]DataAllocator
	entryCount      map[string]int
}

/*
NewCompoundLocationDataAllocator creates a new instance of CompoundLocationDataAllocator
using an EvenDataAllocator as the underlying DataAllocator implementation
*/
func NewCompoundLocationDataAllocator(sizeClasses []int64) *CompoundLocationDataAllocator {
	return &CompoundLocationDataAllocator{
		createAllocator: func() DataAllocator {
			return NewEvenDataAllocator(sizeClasses)
		},
		mutex:      &sync.Mutex{},
		locations:  make(map[string]DataAllocator),
		entryCount: make(map[string]int),
	}
}

// NewEntry creates a new (content, size) entry at a location
func (c *CompoundLocationDataAllocator) NewEntry(loc string, cid string, size int64) error {
	c.mutex.Lock()
	var allocator DataAllocator
	if subAlloc, ok := c.locations[loc]; ok {
		allocator = subAlloc
	} else {
		c.locations[loc] = c.createAllocator()
		c.entryCount[loc] = 0
		allocator = c.locations[loc]
	}
	c.entryCount[loc]++
	c.mutex.Unlock()

	return allocator.NewEntry(cid, size)
}

// DelEntry removes a (content, size) entry from a location
func (c *CompoundLocationDataAllocator) DelEntry(loc string, cid string) error {
	c.mutex.Lock()
	if allocator, ok := c.locations[loc]; ok {
		c.entryCount[loc]--
		if c.entryCount[loc] == 0 {
			delete(c.locations, loc)
			delete(c.entryCount, loc)
		}
		c.mutex.Unlock()
		return allocator.DelEntry(cid)
	}
	c.mutex.Unlock()
	return fmt.Errorf("failed to delete content entry at location(%s) since location non-existant", loc)
}

// AllocateSpace allocates content to an endpoint based on (location, available space)
func (c *CompoundLocationDataAllocator) AllocateSpace(loc string, availableSpace int64) ([]string, error) {
	c.mutex.Lock()
	if allocator, ok := c.locations[loc]; ok {
		c.mutex.Unlock()
		return allocator.AllocateSpace(availableSpace)
	}
	c.mutex.Unlock()
	return nil, fmt.Errorf("failed to allocate space for content at location(%s) since location non-existant", loc)
}

/*
DataAllocator represents an object that can allocate data of a certain
size to different endpoints who are looking to fill up server space
*/
type DataAllocator interface {
	NewEntry(string, int64) error
	DelEntry(string) error
	AllocateSpace(int64) ([]string, error)
}
