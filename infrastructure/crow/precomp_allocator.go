package crow

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

// Represents a Content ID, Priority Value pair that can be sent from and edge server
type ContentPriorityEntry struct {
	Priority int64
	ID       string
}

func updateToPriorityList(update map[string]int64) []ContentPriorityEntry {
	list := make([]ContentPriorityEntry, 0, len(update))
	for key, value := range update {
		list = append(list, ContentPriorityEntry{ID: key, Priority: value})
	}
	return list
}

// List of ContentPriorityEntries for sorting
type contentPriorityList []ContentPriorityEntry

func (c contentPriorityList) Len() int           { return len(c) }
func (c contentPriorityList) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c contentPriorityList) Less(i, j int) bool { return c[i].Priority < c[j].Priority }

/*
PrecomputedDataAllocator implements DataAllocator using a mix of precomputed
allocation lists and on-demand lookups. Ammortized lookup is O(N) when the ratio
N/(requests per precompute job) <= 1 and O((N*log(N))/requests + N) when the ratio
is > 1. N = number of content entities being served on network
*/
type PrecomputedDataAllocator struct {
	mutex *sync.RWMutex

	contentMap       map[string]int64
	dataClasses      []int64
	premadeSolutions []map[string]struct{}

	contentList  []string
	contentSizes []int64
}

/*
NewPrecomputedDataAllocator creates a new PrecomputedDataAllocator that recomputed allocation
lists every 'updateFrequency' using priorities fetch from 'edgeServerAddr'
*/
func NewPrecomputedDataAllocator(edgeServerAddr string, updateFrequency time.Duration,
	dataClasses []int64) (*PrecomputedDataAllocator, error) {
	// Construct api address
	edgeKeyAPI, err := url.JoinPath(edgeServerAddr, infra.DamoclesServiceAPIPriorityListResource)
	if err != nil {
		return nil, err
	}

	// Create allocator object
	allocator := &PrecomputedDataAllocator{
		mutex:            &sync.RWMutex{},
		contentMap:       map[string]int64{},
		dataClasses:      []int64{},
		premadeSolutions: []map[string]struct{}{},
		contentList:      []string{},
		contentSizes:     []int64{},
	}

	// Start recurring precompute job and return
	go startRemoteInfoPrecomputer(edgeKeyAPI, updateFrequency, http.DefaultClient, allocator, dataClasses)
	return allocator, nil
}

// Returns allocation set along with actual size used
func createOptimalAllocation(contentByPrio []ContentPriorityEntry, contentSizeMap map[string]int64, availableSpace int64) (map[string]struct{}, int64) {
	allocations := make(map[string]struct{})

	spaceLeft := availableSpace
	for i := 0; i < len(contentByPrio) && spaceLeft > 0; i++ {
		entry := contentByPrio[i]
		if contentSizeMap[entry.ID] <= spaceLeft {
			spaceLeft -= contentSizeMap[entry.ID]
			allocations[entry.ID] = struct{}{}
		}
	}
	return allocations, availableSpace - spaceLeft
}

type contentSizeInfoList struct {
	ids   []string
	sizes []int64
}

func (s contentSizeInfoList) Len() int { return len(s.ids) }
func (s contentSizeInfoList) Swap(i, j int) {
	s.ids[i], s.ids[j] = s.ids[j], s.ids[i]
	s.sizes[i], s.sizes[j] = s.sizes[j], s.sizes[i]
}
func (s contentSizeInfoList) Less(i, j int) bool {
	return s.sizes[i] < s.sizes[j]
}

func startRemoteInfoPrecomputer(edgePriorityAddress string, frequency time.Duration,
	client *http.Client, allocator *PrecomputedDataAllocator, idealDataClasses []int64) {

	for {
		time.Sleep(frequency)

		// Retrieve updated content allocation priorities from edge server
		allocator.mutex.RLock()
		updateMap := make(map[string]int64)
		err := infra.MakeHTTPRequest(edgePriorityAddress, url.Values{}, nil, client, infra.GOBBodyDecoder, &updateMap)
		if err != nil {
			log.Printf("failed to update content allocation priorities: %s", err.Error())
			continue
		}
		update := updateToPriorityList(updateMap)
		sort.Sort(contentPriorityList(update))

		// Compute size sorted content list for on-demand allocation list filling
		contentList := make([]string, 0, len(allocator.contentMap))
		contentSizes := make([]int64, 0, len(allocator.contentMap))
		for id, size := range allocator.contentMap {
			contentList = append(contentList, id)
			contentSizes = append(contentSizes, size)
		}
		contentInfo := contentSizeInfoList{contentList, contentSizes}
		sort.Sort(contentInfo)

		// Compute priority-value-first premade allocation lists
		dataClasses := make([]int64, len(idealDataClasses))
		premadeSolutions := make([]map[string]struct{}, len(idealDataClasses))
		for i := 0; i < len(idealDataClasses); i++ {
			solutionSet, setSize := createOptimalAllocation(update, allocator.contentMap, idealDataClasses[i])
			dataClasses[i] = setSize
			premadeSolutions[i] = solutionSet
		}
		allocator.mutex.RUnlock()

		// Update allocation structures
		allocator.mutex.Lock()
		allocator.contentList = contentList
		allocator.contentSizes = contentSizes
		allocator.dataClasses = dataClasses
		allocator.premadeSolutions = premadeSolutions
		allocator.mutex.Unlock()
	}

}

/*
NewEntry creates a new id entry for the allocator. Note that id will not start
being served until the next precomputation period
*/
func (b *PrecomputedDataAllocator) NewEntry(id string, size int64) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if _, ok := b.contentMap[id]; ok {
		return fmt.Errorf("failed to add content(%s) to PrecomputedDataAllocator: already exists", id)
	}
	b.contentMap[id] = size
	return nil
}

/*
DelEntry removes a content ID from allocator. Note that id may still
be allocated incorrectly until the next precomputation period
*/
func (b *PrecomputedDataAllocator) DelEntry(id string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if _, ok := b.contentMap[id]; !ok {
		return fmt.Errorf("failed to delete content(%s) from PrecomputedDataAllocator: doesn't exists", id)
	}
	delete(b.contentMap, id)
	return nil
}

// converts map[string]struct{} to a []string
func setToList(set map[string]struct{}) []string {
	list := make([]string, len(set))

	i := 0
	for key := range set {
		list[i] = key
	}
	return list
}

// deep copies a map[string]struct{}
func duplicateSet(set map[string]struct{}) map[string]struct{} {
	dup := make(map[string]struct{})
	for key := range set {
		dup[key] = struct{}{}
	}
	return dup
}

func (b *PrecomputedDataAllocator) AllocateSpace(availableSpace int64) ([]string, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// Lookup precomputed optimal subset of data allocations -> O(log n)
	premadeIdx := approximateBinarySearch(b.dataClasses, availableSpace, false)
	allocationSet := duplicateSet(b.premadeSolutions[premadeIdx])

	// Fill in extra space solely on space restrictions -> O(n)
	availableSpace -= b.dataClasses[premadeIdx]
	contentIdx := approximateBinarySearch(b.contentSizes, availableSpace, false)
	for contentIdx >= 0 && availableSpace > 0 {
		content := b.contentList[contentIdx]
		if _, ok := allocationSet[content]; !ok && availableSpace-b.contentSizes[contentIdx] >= 0 {
			availableSpace -= b.contentSizes[contentIdx]
			allocationSet[content] = struct{}{}
		}
		contentIdx--
	}

	return setToList(allocationSet), nil
}
