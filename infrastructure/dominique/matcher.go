package dominique

import (
  "fmt"
  "time"
  "sync"
  "log"
)

const (
  ReportCollectionFrequency = time.Minute
)

// urlIndex allows looking up what URL a Functional ID is linked to
type urlIndex interface {
  functionalIDToURL(string) (string, error)
}

/* SessionProcessor represents an object that can ingest session reports
and properly match and store the deduced results */
type SessionProcessor interface {
  AddReport(report Report) error
}

type processorEntry struct {
  firstSeen time.Time
  clientReport *ClientReport
  endpointReport *EndpointReport
}

/* TimedSessionProcessor implements SessionProcessor and has a mechanism for
timing out and moving unmatched reports to a secondary storage system for later
processing to prevent OOM */
type TimedSessionProcessor struct {
  reportMatchTimeout time.Duration
  timeseries TimeseriesDB
  finder urlIndex
  mutex *sync.Mutex
  activeSessions map[string]*processorEntry
}

// moving of unmatched reports to secondary storage system
func (t *TimedSessionProcessor) collectUnmatchedReports() {
  t.mutex.Lock()
  defer t.mutex.Unlock()

  for sid, entry := range t.activeSessions {
    if time.Since(entry.firstSeen) > t.reportMatchTimeout {
      if entry.clientReport != nil {
        err := t.timeseries.WriteReport(entry.firstSeen, entry.clientReport)
        if err != nil {
          log.Println(err)
        }
      }
      if entry.endpointReport != nil {
        err := t.timeseries.WriteReport(entry.firstSeen, entry.endpointReport)
        if err != nil {
          log.Println(err)
        }
      }
      delete(t.activeSessions, sid)
    }
  }
}

// NewTimedSessionProcessor creates a new TimedSessionProcessor
func NewTimedSessionProcessor(timeout time.Duration, timeseries TimeseriesDB,
  finder urlIndex) *TimedSessionProcessor {
  processor := &TimedSessionProcessor{
    reportMatchTimeout: timeout,
    timeseries: timeseries,
    finder: finder,
    mutex: &sync.Mutex{},
    activeSessions: make(map[string]*processorEntry),
  }

  // Start unmatched report collector routine
  go func() {
    for {
      time.Sleep(ReportCollectionFrequency)
      processor.collectUnmatchedReports()
    }
  }()

  return processor
}

func (t *TimedSessionProcessor) createSessionDescription(client *ClientReport,
  endpoint *EndpointReport) (SessionDescription, error) {
  desc := SessionDescription{
    SessionID: client.SessionID,
    FunctionalID: client.FunctionalID,
    URL: "",
    ClientIP: client.IP,
    EndpointIP: endpoint.IP,
    EndpointIdentity: endpoint.Identity,
    BytesRecv: client.BytesRecv,
    BytesNeeded: client.BytesNeeded,
  }

  if client.SessionID != endpoint.SessionID {
    return SessionDescription{}, conflictErr
  }
  if client.FunctionalID != endpoint.FunctionalID {
    return SessionDescription{}, conflictErr
  }
  if client.BytesRecv != endpoint.BytesServed {
    return SessionDescription{}, conflictErr
  }

  url, err := t.finder.functionalIDToURL(desc.FunctionalID)
  if err != nil {
    return SessionDescription{}, err
  }
  desc.URL = url
  return desc, nil
}

func (t *TimedSessionProcessor) ingestSessionEntry(entry *processorEntry) error {
  desc, err := t.createSessionDescription(entry.clientReport, entry.endpointReport)
  if err == conflictErr {
    if tErr := t.timeseries.WriteReport(entry.firstSeen, entry.clientReport); tErr != nil {
      return tErr
    }
    if tErr := t.timeseries.WriteReport(entry.firstSeen, entry.endpointReport); tErr != nil {
      return tErr
    }
    return nil
  } else if err != nil {
    return fmt.Errorf("Failed to create session description: %w", err)
  }
  return t.timeseries.WriteDescription(entry.firstSeen, desc)
}

// AddReport adds report r to the system to be processed and matched
func (t *TimedSessionProcessor) AddReport(r Report) error {
  t.mutex.Lock()
  defer t.mutex.Unlock()

  // Create resources if first time seen
  sid := r.GetSessionID()
  entry, ok := t.activeSessions[sid]
  if !ok {
    entry = &processorEntry{
      firstSeen: time.Now(),
      clientReport: nil,
      endpointReport: nil,
    }
    t.activeSessions[sid] = entry
  }

  // Store new report
  switch report := r.(type) {
  case *ClientReport:
    entry.clientReport = report
    break
  case *EndpointReport:
    entry.endpointReport = report
  }

  // Attempt to match
  if entry.clientReport != nil && entry.endpointReport != nil {
    if err := t.ingestSessionEntry(entry); err != nil {
      return fmt.Errorf("Failed to process session reports: %w", err)
    }
    delete(t.activeSessions, sid)
  }
  return nil
}
