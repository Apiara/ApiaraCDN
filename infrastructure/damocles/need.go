package damocles

import (
	"fmt"
	"sync"
	"time"
)

/*
NeedTracker represents an object that can ingest requests and allocations
for an id and return a 'score' where the higher the score, the more in need
of an allocation an 'id' is
*/
type NeedTracker interface {
	GetScore(string) (int64, error)
	CreateCategory(string) error
	DelCategory(string) error
	AddRequest(string) error
	AddAllocation(string) error

	GetSnapshot() map[string]int64
}

// mockNeedTracker is a testing mock for NeedTracker
type mockNeedTracker struct {
	requests  int
	allocates int
}

func (m *mockNeedTracker) GetScore(string) (int64, error) { return 0, nil }
func (m *mockNeedTracker) CreateCategory(string) error    { return nil }
func (m *mockNeedTracker) DelCategory(string) error       { return nil }
func (m *mockNeedTracker) AddRequest(string) error        { m.requests++; return nil }
func (m *mockNeedTracker) AddAllocation(string) error     { m.allocates++; return nil }
func (m *mockNeedTracker) GetSnapshot() map[string]int64  { return map[string]int64{} }

/*
DesperationTracker is a NeedTracker implementation based on self-defined
desperation where desperation = requests - allocations over an active time slice
*/
type DesperationTracker struct {
	activeSlice time.Duration
	lastReset   time.Time
	desperation map[string]int64
	mutex       *sync.Mutex
}

// NewDesperationTracker returns a new DesperationTracker
func NewDesperationTracker(timeSlice time.Duration) *DesperationTracker {
	return &DesperationTracker{
		activeSlice: timeSlice,
		lastReset:   time.Now(),
		desperation: make(map[string]int64),
		mutex:       &sync.Mutex{},
	}
}

// reset sets every key to 0 if the time has moved out of the active time slice
func (d *DesperationTracker) reset() {
	if time.Since(d.lastReset) > d.activeSlice {
		for key := range d.desperation {
			d.desperation[key] = 0
		}
		d.lastReset = time.Now()
	}
}

// GetScore retrieved the desperation score for id
func (d *DesperationTracker) GetScore(id string) (int64, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.reset()
	score, exist := d.desperation[id]
	if !exist {
		return -1, fmt.Errorf("no score for id %s", id)
	}
	return score, nil
}

// CreateCategory creates a tracker category under name id
func (d *DesperationTracker) CreateCategory(id string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, exist := d.desperation[id]; exist {
		return fmt.Errorf("tracker category for %s already exists. cannot create", id)
	}
	d.desperation[id] = 0
	return nil
}

// DelCategory deletes a tracker category by the name of id
func (d *DesperationTracker) DelCategory(id string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if _, exist := d.desperation[id]; !exist {
		return fmt.Errorf("tracker failed to remove category %s. doesn't exist", id)
	}
	delete(d.desperation, id)
	return nil
}

// AddRequest updates id's desperation score with the new request
func (d *DesperationTracker) AddRequest(id string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.reset()
	val, exist := d.desperation[id]
	if !exist {
		return fmt.Errorf("failed to add request for %s. tracker id doesn't exist", id)
	}
	d.desperation[id] = val + 1
	return nil
}

// AddAllocation updates id's' desperation score with the new allocation
func (d *DesperationTracker) AddAllocation(id string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.reset()
	val, exist := d.desperation[id]
	if !exist {
		return fmt.Errorf("failed to track allocation for %s. tracker id doesn't exist", id)
	}
	d.desperation[id] = val - 1
	return nil
}

func (d *DesperationTracker) GetSnapshot() map[string]int64 {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	snapshot := make(map[string]int64)
	for key, value := range d.desperation {
		snapshot[key] = value
	}
	return snapshot
}
