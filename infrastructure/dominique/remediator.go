package dominique

import (
	"errors"
	"fmt"
	"log"
	"time"
)

var (
	unreconcilableErr = errors.New("Failed to reconcile reports")
)

/*
Remediator represents an object that implements a remediation strategy that
tries to create a consistent SessionDescription based on the Client and
Endpoint reports
*/
type Remediator interface {
	Reconcile(ClientReport, EndpointReport) (*SessionDescription, error)
}

func StartRemediaton(frequency time.Duration, timeseries TimeseriesDB, remediators []Remediator) {
	lastProcessTime := time.Now()
	for {
		time.Sleep(frequency)

		// Read all reports that have been written since the last batch process
		currentProcessTime := time.Now()
		unmatchedReports, err := timeseries.ReadReportRange(lastProcessTime, currentProcessTime)
		if err != nil {
			log.Printf("Failed to read from unmatched reports bucket: %v\n", err)
			continue
		}

		// Process all new reports
		processed := make(map[string]struct{})
		for _, report := range unmatchedReports {
			// Handle case that session has already been processed
			sessionID := report.GetSessionID()
			if _, ok := processed[sessionID]; ok {
				continue
			}

			// Attempt to handle session associated with report
			err := processSession(sessionID, time.Time{}, currentProcessTime, timeseries, remediators)
			if err != nil {
				log.Printf("Failed to process reports for session %s: %v\n", sessionID, err)
			}
			processed[sessionID] = struct{}{}
		}
		lastProcessTime = currentProcessTime
	}
}

func processSession(sid string, startTime time.Time, endTime time.Time,
	timeseries TimeseriesDB, remediators []Remediator) error {
	// Read reports associated with the session ID
	clientReport, endpointReport, err := timeseries.ReadSessionReports(sid, startTime, endTime)
	if err != nil {
		return err
	}

	// Attempt all remediation strategies on reports
	for _, remediator := range remediators {
		description, err := remediator.Reconcile(*clientReport, *endpointReport)
		if err == nil {
			err = timeseries.WriteDescription(*description, time.Now())
			if err != nil {
				return fmt.Errorf("Failed to write description post-reconciliation: %w", err)
			}
			return nil
		} else if err != nil && err != unreconcilableErr {
			log.Printf("Failed to reconcile reports: %v", err)
		}
	}
	return unreconcilableErr
}
