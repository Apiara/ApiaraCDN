package dominique

import (
	"errors"
	"fmt"
	"log"
	"time"
)

var (
	errUnreconcilable = errors.New("failed to reconcile reports")
)

/*
StartRemediation starts a job that batch processes all reports that were
unable to be matched by the on-demand SessionProcessor. This job runs every
'frequency' time period. All jobs that can't be processed/remediated are
written to 'handleQueue' to be handled by a human for manual remediation.
*/
func StartRemediaton(frequency time.Duration, timeseries TimeseriesDB,
	remediators []Remediator, handleQueue ManualRemediationQueue) {
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
			err := processSession(sessionID, time.Time{}, currentProcessTime, timeseries, remediators, handleQueue)
			if err != nil {
				log.Printf("Failed to process reports for session %s: %v\n", sessionID, err)
			}
			processed[sessionID] = struct{}{}
		}
		lastProcessTime = currentProcessTime
	}
}

func processSession(sid string, startTime time.Time, endTime time.Time,
	timeseries TimeseriesDB, remediators []Remediator, handleQueue ManualRemediationQueue) error {
	// Read reports associated with the session ID
	clientReport, endpointReport, err := timeseries.ReadSessionReports(sid, startTime, endTime)
	if err != nil {
		return err
	}

	// Attempt all automated remediation strategies on reports
	for _, remediator := range remediators {
		description, err := remediator.Reconcile(clientReport, endpointReport)
		if err == nil {
			err = timeseries.WriteDescription(description, time.Now())
			if err != nil {
				return fmt.Errorf("failed to write description post-reconciliation: %w", err)
			}
			return nil
		} else if err != nil && err != errUnreconcilable {
			log.Printf("Failed to reconcile reports: %v", err)
		}
	}

	// Write reports to queue for manual remediation if no remediation strategies worked
	if err = handleQueue.Write(clientReport, endpointReport); err != nil {
		return fmt.Errorf("failed to write reports to manual remediation queue: %w", err)
	}
	return nil
}
