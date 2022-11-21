package deus

import (
  "net/http"
  "net/url"
  "time"
  "fmt"
  "encoding/json"
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

// MasterContentManager implements ContentManager
type MasterContentManager struct {
  serveState ContentState
  httpClient *http.Client
  processAPIAddr string
  coordinateAPIAddr string
}

func (m *MasterContentManager) processContent(cid string) (string, error) {
  type StatusResponse struct {
    Status string `json:"Status"`
    FunctionalID *string `json:"FunctionalID"`
  }

  // Create data process request
  processReq, err := http.NewRequest("GET", m.processAPIAddr + "/process", nil)
  if err != nil {
    return "", err
  }

  query := url.Values{}
  query.Add(ContentIDHeader, cid)
  processReq.URL.RawQuery = query.Encode()

  resp, err := m.httpClient.Do(processReq)
  if err != nil {
    return "", err
  }

  if resp.StatusCode != http.StatusOK {
    return "", fmt.Errorf("Process request failed with error code %d", resp.StatusCode)
  }

  // Poll for terminal status
  statusReq, err := http.NewRequest("GET", m.processAPIAddr + "/status", nil)
  if err != nil {
    return "", err
  }
  statusReq.URL.RawQuery = query.Encode()

  status := StatusResponse{}
  startTime := time.Now()
  for time.Since(startTime) < ProcessStatusTimeout {
    // Sleep for poll time
    time.Sleep(ProcessStatusPollFrequency)

    // Request and decode status update
    resp, err = m.httpClient.Do(statusReq)
    if err != nil {
      return "", err
    }
    if resp.StatusCode != http.StatusOK {
      fmt.Errorf("Process status request failed with error code %d", resp.StatusCode)
      continue
    }
    if err = json.NewDecoder(resp.Body).Decode(&status); err != nil {
      return "", err
    }

    // Check state
    switch status.Status {
    case "running":
      continue
    case "failed":
      return "", fmt.Errorf("Process request for %s failed", cid)
    case "complete":
      return *(status.FunctionalID), nil
    }
  }

  return "", fmt.Errorf("Process request for %s timed out", cid)
}

func (m *MasterContentManager) deleteProcessedContent(cid string) error {
  // Create data removal request
  deleteReq, err := http.NewRequest("GET", m.processAPIAddr + "/delete", nil)
  if err != nil {
    return err
  }

  query := url.Values{}
  query.Add(ContentIDHeader, cid)
  deleteReq.URL.RawQuery = query.Encode()

  // Perform removal request
  resp, err := m.httpClient.Do(deleteReq)
  if err != nil {
    return err
  }
  if resp.StatusCode != http.StatusOK {
    return fmt.Errorf("Removal request for %s failed with status code %d", cid, resp.StatusCode)
  }
  return nil
}

func (m *MasterContentManager) publishContent(cid string, functionlID string) error {
  // Create content publishing request
  publishReq, err := http.NewRequest("GET", m.coordinateAPIAddr + "/publish", nil)
  if err != nil {
    return err
  }

  query := url.Values{}
  query.Add(ContentIDHeader, cid)
  query.Add(FunctionalIDHeader, functionlID)
  publishReq.URL.RawQuery = query.Encode()

  // Perform content publishing request
  resp, err := m.httpClient.Do(publishReq)
  if err != nil {
    return err
  }
  if resp.StatusCode != http.StatusOK {
    return fmt.Errorf("Publish request for %s failed with error code %d", cid, resp.StatusCode)
  }
  return nil
}

func (m *MasterContentManager) purgeContent(cid string) error {
  // Create content purge request
  purgeReq, err := http.NewRequest("GET", m.coordinateAPIAddr + "/purge", nil)
  if err != nil {
    return err
  }

  query := url.Values{}
  query.Add(ContentIDHeader, cid)
  purgeReq.URL.RawQuery = query.Encode()

  // Perform content purge request
  resp, err := m.httpClient.Do(purgeReq)
  if err != nil {
    return err
  }
  if resp.StatusCode != http.StatusOK {
    return fmt.Errorf("Purge request for %s failed with error code %d", cid, resp.StatusCode)
  }
  return nil
}

// Serve attempts serve 'cid' on the network
func (m *MasterContentManager) Serve(cid string, serverAddr string, dynamic bool) error {
  // Process content
  functionalID, err := m.processContent(cid)
  if err != nil {
    return err
  }

  // Publish content to coordination infrastructure
  if err = m.publishContent(cid, functionalID); err != nil {
    // Reverse content processing; remove processed content from data stores
    m.deleteProcessedContent(cid)
    return err
  }

  // Update global state
  return m.serveState.Set(cid, serverAddr, dynamic)
}

/* Remove attempts to delete and purge 'cid' from the network. This can fail
if the content was pushed onto the network manually and the remove request
was performed dynamically since only a manual removal request can purge data
that was manually pushed*/
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
  if err := m.purgeContent(cid); err != nil {
    return err
  }

  // Delete processed data
  if err := m.deleteProcessedContent(cid); err != nil {
    return err
  }
  return nil
}
