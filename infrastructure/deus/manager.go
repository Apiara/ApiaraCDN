package deus

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
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
	Serve(cid string, serverAddr string, dynamic bool) error
	Remove(cid string, serverAddr string, dynamic bool) error
}

// mockContentManager is a mock implementation for testing
type mockContentManager struct {
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

// MasterContentManager implements ContentManager
type MasterContentManager struct {
	serveState        ContentLocationIndex
	dataIndex         infra.DataIndex
	httpClient        *http.Client
	processAPIAddr    string
	coordinateAPIAddr string
}

/*
NewMasterContentManager returns a new instances of MasterContentManager
that uses the processAPI and coordinateAPI to delegate tasks
*/
func NewMasterContentManager(serverState ContentLocationIndex, index infra.DataIndex, processAPI string,
	coordinateAPI string) *MasterContentManager {
	return &MasterContentManager{
		serveState:        serverState,
		dataIndex:         index,
		httpClient:        http.DefaultClient,
		processAPIAddr:    processAPI,
		coordinateAPIAddr: coordinateAPI,
	}
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
	err := m.sendHTTPMessage(m.processAPIAddr+"/process", query.Encode())
	if err != nil {
		return "", -1, err
	}

	// Poll for terminal status
	statusReq, err := http.NewRequest("GET", m.processAPIAddr+"/status", nil)
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
	return m.sendHTTPMessage(m.processAPIAddr+"/delete", query.Encode())
}

func (m *MasterContentManager) publishContent(serverAddr string, functionlID string, size int64) error {
	// Inform session server of new data to serve
	query := url.Values{}
	query.Add(infra.FunctionalIDHeader, functionlID)

	err := m.sendHTTPMessage(serverAddr+"/category/add", query.Encode())
	if err != nil {
		return err
	}

	// Perform content publishing request to dataspace allocator
	query.Add(infra.ByteSizeHeader, strconv.FormatInt(size, 10))
	err = m.sendHTTPMessage(m.coordinateAPIAddr+"/publish", query.Encode())
	if err != nil {
		return err
	}

	return nil
}

func (m *MasterContentManager) stopServing(serverAddr string, cid string) error {
	fid, err := m.dataIndex.GetFunctionalID(cid)
	if err != nil {
		return err
	}

	// Send purge request to session server
	query := url.Values{}
	query.Add(infra.FunctionalIDHeader, fid)
	err = m.sendHTTPMessage(serverAddr+"/category/del", query.Encode())
	if err != nil {
		return err
	}

	return nil
}

func (m *MasterContentManager) unpublishContent(cid string) error {
	fid, err := m.dataIndex.GetFunctionalID(cid)
	if err != nil {
		return err
	}

	// Send purge request to coordination layer
	query := url.Values{}
	query.Add(infra.FunctionalIDHeader, fid)
	err = m.sendHTTPMessage(m.coordinateAPIAddr+"/purge", query.Encode())
	if err != nil {
		return err
	}
	return nil
}

// Serve attempts serve 'cid' on the network
func (m *MasterContentManager) Serve(cid string, serverAddr string, dynamic bool) error {
	// Check if content has been processed yet
	processed, err := m.serveState.IsBeingServed(cid)
	if err != nil {
		return err
	}

	// Get functional id and processed content size. Process if not processed
	var functionalID string
	var size int64
	if !processed {
		functionalID, size, err = m.processContent(cid)
		if err != nil {
			return err
		}
	} else {
		functionalID, err = m.dataIndex.GetFunctionalID(cid)
		if err != nil {
			return err
		}
		size, err = m.dataIndex.GetSize(cid)
		if err != nil {
			return err
		}
	}

	// Publish content to coordination infrastructure
	if err = m.publishContent(serverAddr, functionalID, size); err != nil {
		return err
	}

	// Update global state
	return m.serveState.Set(cid, serverAddr, dynamic)
}

/*
Remove attempts to delete and purge 'cid' from the network. This can fail
if the content was pushed onto the network manually and the remove request
was performed dynamically since only a manual removal request can purge data
that was manually pushed
*/
func (m *MasterContentManager) Remove(cid string, serverAddr string, dynamic bool) error {
	// Update state
	dynamicallySet, err := m.serveState.WasDynamicallySet(cid, serverAddr)
	if err != nil {
		return err
	}

	if dynamic && !dynamicallySet {
		return fmt.Errorf("Cannot dynamically remove %s from %s since it was manually pushed", cid, serverAddr)
	}
	if err := m.serveState.Remove(cid, serverAddr); err != nil {
		return err
	}

	// Purge from coordination infrastructure
	if err := m.stopServing(serverAddr, cid); err != nil {
		return err
	}

	// Delete processed data if no longer being served anywhere on the network
	inUse, err := m.serveState.IsBeingServed(cid)
	if err != nil {
		return err
	}

	if !inUse {
		if err := m.deleteProcessedContent(cid); err != nil {
			return err
		}
		if err := m.unpublishContent(cid); err != nil {
			return err
		}
	}
	return nil
}
