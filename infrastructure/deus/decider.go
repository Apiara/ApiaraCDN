package deus

import (
  "sync"
  "time"
  "strings"
  "log"
)


// PullDecider makes decisions about what content to pull based on
// request frequency
type PullDecider interface {
  NewRequest(cid string, serverID string)
}

/* ThresholdPullDecider uses information about the frequency of content
requests in different regions to pull data to be dynamically served by
the network. A threshold number of requests/time is used to decide whether
or not a piece of content will be pulled*/
type ThresholdPullDecider struct {
  validator ContentValidator
  requestCounts map[string]int
  mutex *sync.Mutex
}

func generateServePairKey(cid string, serverID string) string {
  return serverID + "|" + cid
}

func unpackServePairKey(key string) (string, string) {
  cid, serverID, _ := strings.Cut(key, "|")
  return cid, serverID
}

/* NewThresholdPullDecider creates a new ThresholdPullDecider and starts the
decision thread with the passed in requestThreshold and decisionInterval params */
func NewThresholdPullDecider(validator ContentValidator, contentManager ContentManager,
  dataState ContentState, requestThreshold int, decisionInterval time.Duration) *ThresholdPullDecider {

  // Create ThresholdPullDecider objects
  decider := &ThresholdPullDecider{
    validator: validator,
    requestCounts: make(map[string]int),
    mutex: &sync.Mutex{},
  }

  // Start pull decider go routine
  go func() {
    for {
      time.Sleep(decisionInterval)
      decider.mutex.Lock()
      for key, count := range decider.requestCounts {
        cid, serverAddr := unpackServePairKey(key)
        // Add data if above threshold and not being served

        serving, err := dataState.IsBeingServed(cid, serverAddr)
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
            log.Println(err)
          }
        }
      }
      decider.mutex.Unlock()
    }
  }()

  return decider
}

// NewRequest logs a request that was made for future pull decisions
func (t *ThresholdPullDecider) NewRequest(cid string, serverID string) {
  if t.validator.MatchesPrefixRule(cid) {
    key := generateServePairKey(cid, serverID)
    t.mutex.Lock()
    t.requestCounts[key]++
    t.mutex.Unlock()
  }
}