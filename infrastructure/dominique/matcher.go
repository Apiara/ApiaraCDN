package dominique

import (
	"fmt"
	"log"
	"sync"
	"time"
)

var (
	// Frequency at which active reports are checked to see if they've timed out
	ReportCollectionFrequency = time.Minute
)

/*
SessionProcessor represents an object that can ingest session reports
and properly match and store the deduced results
*/
type SessionProcessor interface {
	AddReport(report Report) error
}

type processorEntry struct {
	firstSeen      time.Time
	clientReport   *ClientReport
	endpointReport *EndpointReport
}

/*
TimedSessionProcessor implements SessionProcessor and has a mechanism for
timing out and moving unmatched reports to a secondary storage system for later
processing to prevent OOM
*/
type TimedSessionProcessor struct {
	reportMatchTimeout time.Duration
	timeseries         TimeseriesDBWriter
	mutex              *sync.Mutex
	activeSessions     map[string]*processorEntry
}

// moving of unmatched reports to secondary storage system
func (t *TimedSessionProcessor) collectUnmatchedReports() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for sid, entry := range t.activeSessions {
		if time.Since(entry.firstSeen) > t.reportMatchTimeout {
			if entry.clientReport != nil && entry.endpointReport != nil {
				/* Try to process complete entries here for a final time if previous
				   processing failed. If fails again, remove from active sessions without
				   logging */
				err := t.ingestSessionEntry(entry)
				if err != nil {
					log.Println(err)
				}
			} else if entry.clientReport != nil {
				err := t.timeseries.WriteReport(entry.clientReport, entry.firstSeen)
				if err != nil {
					log.Println(err)
				}
			} else if entry.endpointReport != nil {
				err := t.timeseries.WriteReport(entry.endpointReport, entry.firstSeen)
				if err != nil {
					log.Println(err)
				}
			}
			delete(t.activeSessions, sid)
		}
	}
}

// NewTimedSessionProcessor creates a new TimedSessionProcessor
func NewTimedSessionProcessor(timeout time.Duration, timeseries TimeseriesDBWriter) *TimedSessionProcessor {
	processor := &TimedSessionProcessor{
		reportMatchTimeout: timeout,
		timeseries:         timeseries,
		mutex:              &sync.Mutex{},
		activeSessions:     make(map[string]*processorEntry),
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

func createSessionDescription(client ClientReport,
	endpoint EndpointReport) (SessionDescription, error) {
	if client.SessionID != endpoint.SessionID {
		return SessionDescription{}, errConflict
	}
	if client.FunctionalID != endpoint.FunctionalID {
		return SessionDescription{}, errConflict
	}
	if client.BytesRecv != endpoint.BytesServed {
		return SessionDescription{}, errConflict
	}

	desc := SessionDescription{
		SessionID:        client.SessionID,
		FunctionalID:     client.FunctionalID,
		ClientIP:         client.IP,
		EndpointIP:       endpoint.IP,
		EndpointIdentity: endpoint.Identity,
		BytesRecv:        client.BytesRecv,
		BytesNeeded:      client.BytesNeeded,
		Agree:            true,
	}
	return desc, nil
}

func (t *TimedSessionProcessor) ingestSessionEntry(entry *processorEntry) error {
	desc, err := createSessionDescription(*entry.clientReport, *entry.endpointReport)
	if err == errConflict {
		if tErr := t.timeseries.WriteReport(entry.clientReport, entry.firstSeen); tErr != nil {
			return tErr
		}
		if tErr := t.timeseries.WriteReport(entry.endpointReport, entry.firstSeen); tErr != nil {
			return tErr
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to create session description: %w", err)
	}
	return t.timeseries.WriteDescription(desc, entry.firstSeen)
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
			firstSeen:      time.Now(),
			clientReport:   nil,
			endpointReport: nil,
		}
		t.activeSessions[sid] = entry
	}

	// Store new report
	switch report := r.(type) {
	case *ClientReport:
		entry.clientReport = report
	case *EndpointReport:
		entry.endpointReport = report
	}

	// Attempt to match
	if entry.clientReport != nil && entry.endpointReport != nil {
		if err := t.ingestSessionEntry(entry); err != nil {
			/* If fails, will get picked up again by the scheduled
			   collectUnmatchedReports routine for a second try at processing
			   before throwing the entry away for good*/
			return fmt.Errorf("failed to process session reports: %w", err)
		}
		delete(t.activeSessions, sid)
	}
	return nil
}
