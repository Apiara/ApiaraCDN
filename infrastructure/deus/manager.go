package deus

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

var (
	// How often the status of a data processing job is checked
	ProcessStatusPollFrequency = time.Second

	// Max time allocated to a data processing job before it is discarded
	ProcessStatusTimeout = time.Minute * 5
)

// ContentManager controls serving and removing data from the network
type ContentManager interface {
	Serve(cid string, regionID string, dynamic bool) error
	Remove(cid string, regionID string, dynamic bool) error
	Lock()
	Unlock()
}

// mockContentManager is a mock implementation for testing
type mockContentManager struct {
	mutex   *sync.Mutex
	serving map[string]bool
}

func (m *mockContentManager) Serve(cid string, server string, dyn bool) error {
	m.serving[cid+server] = dyn
	return nil
}

func (m *mockContentManager) Remove(cid string, server string, dyn bool) error {
	delete(m.serving, cid+server)
	return nil
}

func (m *mockContentManager) Lock()   { m.mutex.Lock() }
func (m *mockContentManager) Unlock() { m.mutex.Unlock() }

// MasterContentManager implements ContentManager
type MasterContentManager struct {
	mutex                *sync.Mutex
	state                ManagerMicroserviceState
	httpClient           *http.Client
	processDataAPIAddr   string
	processStatusAPIAddr string
	deleteDataAPIAddr    string
	publishDataAPIAddr   string
	unpublishDataAPIAddr string
}

/*
NewMasterContentManager returns a new instances of MasterContentManager
that uses the processAPI and coordinateAPI to delegate tasks
*/
func NewMasterContentManager(state ManagerMicroserviceState, processAPI string,
	coordinateAPI string) (*MasterContentManager, error) {
	// Prepare API resources
	processDataAPIAddr, err := url.JoinPath(processAPI, infra.CyprusServiceAPIProcessResource)
	if err != nil {
		return nil, err
	}
	processStatusAPIAddr, err := url.JoinPath(processAPI, infra.CyprusServiceAPIStatusResource)
	if err != nil {
		return nil, err
	}
	deleteDataAPIAddr, err := url.JoinPath(processAPI, infra.CyprusServiceAPIDeleteResource)
	if err != nil {
		return nil, err
	}
	publishDataAPIAddr, err := url.JoinPath(coordinateAPI, infra.CrowServiceAPIPublishResource)
	if err != nil {
		return nil, err
	}
	unpublishDataAPIAddr, err := url.JoinPath(coordinateAPI, infra.CrowServiceAPIPurgeResource)
	if err != nil {
		return nil, err
	}

	return &MasterContentManager{
		mutex:                &sync.Mutex{},
		state:                state,
		httpClient:           http.DefaultClient,
		processDataAPIAddr:   processDataAPIAddr,
		processStatusAPIAddr: processStatusAPIAddr,
		deleteDataAPIAddr:    deleteDataAPIAddr,
		publishDataAPIAddr:   publishDataAPIAddr,
		unpublishDataAPIAddr: unpublishDataAPIAddr,
	}, nil
}

func (m *MasterContentManager) sendHTTPMessage(addr string, query string) error {
	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		return err
	}

	req.URL.RawQuery = query
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Received non-successful http response: %d", resp.StatusCode)
	}
	return nil
}

func (m *MasterContentManager) processContent(cid string) (string, int64, error) {
	// Create data process request
	query := url.Values{}
	query.Add(infra.ContentIDHeader, cid)
	err := m.sendHTTPMessage(m.processDataAPIAddr, query.Encode())
	if err != nil {
		return "", -1, err
	}

	// Poll for terminal status
	statusReq, err := http.NewRequest("GET", m.processStatusAPIAddr, nil)
	if err != nil {
		return "", -1, err
	}
	statusReq.URL.RawQuery = query.Encode()

	status := infra.StatusResponse{}
	startTime := time.Now()
	for time.Since(startTime) < ProcessStatusTimeout {
		// Sleep for poll time
		time.Sleep(ProcessStatusPollFrequency)

		// Request and decode status update
		resp, err := m.httpClient.Do(statusReq)
		if err != nil {
			return "", -1, err
		}
		if resp.StatusCode != http.StatusOK {
			return "", -1, fmt.Errorf("Process status request failed with error code %d", resp.StatusCode)
		}
		if err = json.NewDecoder(resp.Body).Decode(&status); err != nil {
			return "", -1, err
		}

		// Check state
		switch status.Status {
		case infra.RunningProcessing:
			continue
		case infra.FailedProcessing:
			return "", -1, fmt.Errorf("Process request for %s failed", cid)
		case infra.FinishedProcessing:
			return status.Metadata.FunctionalID, status.Metadata.ByteSize, nil
		}
	}

	return "", -1, fmt.Errorf("Process request for %s timed out", cid)
}

func (m *MasterContentManager) deleteProcessedContent(cid string) error {
	query := url.Values{}
	query.Add(infra.ContentIDHeader, cid)
	return m.sendHTTPMessage(m.deleteDataAPIAddr, query.Encode())
}

func (m *MasterContentManager) startServingAtEdge(edgeServerAddr string, functionalID string) error {
	query := url.Values{}
	query.Add(infra.ContentFunctionalIDHeader, functionalID)
	serverAddResource, err := url.JoinPath(edgeServerAddr, infra.DamoclesServiceAPIAddResource)
	if err != nil {
		return err
	}
	if err = m.sendHTTPMessage(serverAddResource, query.Encode()); err != nil {
		return err
	}
	return nil
}

func (m *MasterContentManager) publishContentToAllocator(regionID string, functionalID string, size int64) error {
	// Perform content publishing request to dataspace allocator
	query := url.Values{}
	query.Add(infra.ContentFunctionalIDHeader, functionalID)
	query.Add(infra.ByteSizeHeader, strconv.FormatInt(size, 10))
	query.Add(infra.RegionServerIDHeader, regionID)
	if err := m.sendHTTPMessage(m.publishDataAPIAddr, query.Encode()); err != nil {
		return err
	}

	return nil
}

func (m *MasterContentManager) stopServingAtEdge(serverAddr string, functionalID string) error {
	// Send purge request to session server
	query := url.Values{}
	query.Add(infra.ContentFunctionalIDHeader, functionalID)

	serverDelResource, err := url.JoinPath(serverAddr, infra.DamoclesServiceAPIDelResource)
	if err != nil {
		return err
	}
	err = m.sendHTTPMessage(serverDelResource, query.Encode())
	if err != nil {
		return err
	}

	return nil
}

func (m *MasterContentManager) unpublishContentFromAllocator(regionID string, functionalID string) error {
	// Send purge request to allocation server
	query := url.Values{}
	query.Add(infra.ContentFunctionalIDHeader, functionalID)
	query.Add(infra.RegionServerIDHeader, regionID)
	if err := m.sendHTTPMessage(m.unpublishDataAPIAddr, query.Encode()); err != nil {
		return err
	}
	return nil
}

func performRollback(reversalOperations []func() error) {
	// Perform rollback LIFO
	for i := len(reversalOperations) - 1; i >= 0; i-- {
		reverse := reversalOperations[i]
		if err := reverse(); err != nil {
			// Log errors, still attempt to perform every rollback operation
			log.Printf("failed rollback operation: %s\n", err.Error())
		}
	}
}

// Serve attempts serve 'cid' on the network
func (m *MasterContentManager) Serve(cid string, regionID string, dynamic bool) error {
	/* Every time a operation is completed store the inverse
	operation in case rollback needs to be performed */
	rollbackOperations := make([]func() error, 0)

	// Check if content has been processed yet
	processed, err := m.state.IsContentBeingServed(cid)
	if err != nil {
		return err
	}

	// Get functional id and processed content size. Process if not processed
	var functionalID string
	var size int64
	if !processed {
		// Attempt content processing, update rollback operations
		functionalID, size, err = m.processContent(cid)
		if err != nil {
			return err
		}
		rollbackOperations = append(rollbackOperations, func() error {
			return m.deleteProcessedContent(cid)
		})
	} else {
		functionalID, err = m.state.GetContentFunctionalID(cid)
		if err != nil {
			return err
		}
		size, err = m.state.GetContentSize(cid)
		if err != nil {
			return err
		}
	}

	// Publish content to coordination infrastructure
	serverAddr, err := m.state.GetServerPrivateAddress(regionID)
	if err != nil {
		performRollback(rollbackOperations)
		return err
	}

	// Attempt updating allocator service, update rollback operation list
	if err = m.publishContentToAllocator(regionID, functionalID, size); err != nil {
		performRollback(rollbackOperations)
		return err
	}
	rollbackOperations = append(rollbackOperations, func() error {
		return m.unpublishContentFromAllocator(regionID, functionalID)
	})

	// Attempt updating edge server, update rollback operation list
	if err = m.startServingAtEdge(serverAddr, functionalID); err != nil {
		performRollback(rollbackOperations)
		return err
	}
	rollbackOperations = append(rollbackOperations, func() error {
		return m.stopServingAtEdge(serverAddr, functionalID)
	})

	// Update global state
	if err = m.state.CreateContentLocationEntry(cid, regionID, dynamic); err != nil {
		performRollback(rollbackOperations)
		return err
	}
	return nil
}

/*
Remove attempts to delete and purge 'cid' from the network. This can fail
if the content was pushed onto the network manually and the remove request
was performed dynamically since only a manual removal request can purge data
that was manually pushed
*/
func (m *MasterContentManager) Remove(cid string, regionID string, dynamic bool) error {
	// Keep track of operations needed for rollback
	rollbackOperations := make([]func() error, 0)

	// Update state
	dynamicallySet, err := m.state.WasContentPulled(cid, regionID)
	if err != nil {
		return err
	}
	serverAddr, err := m.state.GetServerPrivateAddress(regionID)
	if err != nil {
		return err
	}
	functionalID, err := m.state.GetContentFunctionalID(cid)
	if err != nil {
		return err
	}
	contentSize, err := m.state.GetContentSize(cid)
	if err != nil {
		return err
	}

	// Remove metadata location entry, update rollback operations
	if dynamic && !dynamicallySet {
		return fmt.Errorf("cannot dynamically remove %s from %s since it was manually pushed", cid, regionID)
	}
	if err := m.state.DeleteContentLocationEntry(cid, regionID); err != nil {
		return err
	}
	rollbackOperations = append(rollbackOperations, func() error {
		return m.state.CreateContentLocationEntry(cid, regionID, dynamicallySet)
	})

	// Purge from edge server, update rollback list
	if err := m.stopServingAtEdge(serverAddr, functionalID); err != nil {
		performRollback(rollbackOperations)
		return err
	}
	rollbackOperations = append(rollbackOperations, func() error {
		return m.startServingAtEdge(serverAddr, functionalID)
	})

	// Purge from allocator service, update rollback list
	if err := m.unpublishContentFromAllocator(regionID, functionalID); err != nil {
		performRollback(rollbackOperations)
		return err
	}
	rollbackOperations = append(rollbackOperations, func() error {
		return m.publishContentToAllocator(regionID, functionalID, contentSize)
	})

	// Delete processed data if no longer being served anywhere on the network
	inUse, err := m.state.IsContentBeingServed(cid)
	if err != nil {
		performRollback(rollbackOperations)
		return err
	}

	if !inUse {
		if err := m.deleteProcessedContent(cid); err != nil {
			performRollback(rollbackOperations)
			return err
		}
	}
	return nil
}

/*
Use Lock every time Set, Remove, or any combination of MasterContentManager calls are
made that are supposed to be a single unit(ie. a transaction)
*/
func (m *MasterContentManager) Lock() {
	m.mutex.Lock()
}

// Call Unlock when you are done with a MasterContentManager transaction
func (m *MasterContentManager) Unlock() {
	m.mutex.Unlock()
}
