package deus

import (
	"log"
	"strings"
	"sync"
	"time"
)

// PullDecider makes decisions about what content to pull based on
// request frequency
type PullDecider interface {
	NewRequest(cid string, serverID string) error
}

// mockPullDecider is a PullDecider mock for testing
type mockPullDecider struct{}

func (m *mockPullDecider) NewRequest(string, string) error { return nil }

/*
ThresholdPullDecider uses information about the frequency of content
requests in different regions to pull data to be dynamically served by
the network. A threshold number of requests/time is used to decide whether
or not a piece of content will be pulled
*/
type ThresholdPullDecider struct {
	validator     ContentValidator
	requestCounts map[string]int
	mutex         *sync.Mutex
}

func generateServePairKey(cid string, serverID string) string {
	return cid + "|" + serverID
}

func unpackServePairKey(key string) (string, string) {
	cid, serverID, _ := strings.Cut(key, "|")
	return cid, serverID
}

/*
NewThresholdPullDecider creates a new ThresholdPullDecider and starts the
decision thread with the passed in requestThreshold and decisionInterval params
*/
func NewThresholdPullDecider(validator ContentValidator, contentManager ContentManager,
	dataState ContentLocationIndex, requestThreshold int, decisionInterval time.Duration) *ThresholdPullDecider {

	// Create ThresholdPullDecider objects
	decider := &ThresholdPullDecider{
		validator:     validator,
		requestCounts: make(map[string]int),
		mutex:         &sync.Mutex{},
	}

	// Start pull decider go routine
	go func() {
		for {
			time.Sleep(decisionInterval)
			decider.mutex.Lock()
			for key, count := range decider.requestCounts {
				cid, serverAddr := unpackServePairKey(key)
				// Add data if above threshold and not being served

				serving, err := dataState.IsServedByServer(cid, serverAddr)
				if err != nil {
					log.Println(err)
					continue
				}

				if !serving {
					if count > requestThreshold {
						if err := contentManager.Serve(cid, serverAddr, true); err != nil {
							log.Println(err)
						}
					}
				} else if count < requestThreshold { // Remove data if below threshold and being served
					if err := contentManager.Remove(cid, serverAddr, true); err != nil {
						delete(decider.requestCounts, key)
						log.Println(err)
					}
				}
			}

			// Reset map
			for key := range decider.requestCounts {
				decider.requestCounts[key] = 0
			}
			decider.mutex.Unlock()
		}
	}()

	return decider
}

// NewRequest logs a request that was made for future pull decisions
func (t *ThresholdPullDecider) NewRequest(cid string, serverID string) error {
	isValid, err := t.validator.IsValid(cid)
	if err != nil {
		return err
	}

	if isValid {
		key := generateServePairKey(cid, serverID)
		t.mutex.Lock()
		t.requestCounts[key]++
		t.mutex.Unlock()
	}
	return nil
}
