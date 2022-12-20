package dominique

import (
	"context"
	"fmt"
	"time"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxAPI "github.com/influxdata/influxdb-client-go/v2/api"
)

var (
	// InfluxDB organization name
	OrganizationName = "dominique"

	// InfluxDB database for matched sessions
	SessionsBucket = "matched_sessions"
	//InfluxDB bucket for unmatched or conflicted reports
	ReportsBucket = "unmatched_reports"
)

const (
	// Common line protocol keys
	sessionIDTag      = "session_id"
	contentIDTag      = "content_id"
	functionalIDField = "functional_id"
	endpointIDTag     = "endpoint_id"
	bytesRecvField    = "bytes_recv"
	bytesNeededField  = "bytes_needed"

	// SesionsBucket entry line protocol keys
	clientIPField     = "client_ip"
	endpointIPField   = "endpoint_ip"
	reportsAgreeField = "agreed"

	// ReportsBucket entry line protocol keys
	clientReportMeasure   = "client"
	endpointReportMeasure = "endpoint"
	reportIPField         = "ip"
	bytesServedField      = "bytes_served"
)

/*
TimeseriesDBWriter represents an object that can store Reports
and SessionDescriptions as a timeseries of events
*/
type TimeseriesDBWriter interface {
	WriteReport(r Report, t time.Time) error
	WriteDescription(desc SessionDescription, t time.Time) error
}

/*
TimeseriesDBReader represents an object that can retrieve stored
Reports and SessionDescriptions by time range
*/
type TimeseriesDBReader interface {
	ReadReportRange(start time.Time, end time.Time) ([]Report, error)
	ReadSessionReports(sid string, start time.Time, end time.Time) (*ClientReport, *EndpointReport, error)
	ReadEndpointSessions(eid string, start time.Time, end time.Time) ([]SessionDescription, error)
	ReadContentSessions(cid string, start time.Time, end time.Time) ([]SessionDescription, error)
}

// TimeseriesDB is a combination of TimeseriesDBReader and TimeseriesDBWriter
type TimeseriesDB interface {
	TimeseriesDBReader
	TimeseriesDBWriter
}

// mockTimeseriesDB is a testing mock for TimeseriesDB
type mockTimeseriesDB struct {
	reports map[string][]Report
	descs   map[string][]SessionDescription
}

func (m *mockTimeseriesDB) WriteReport(r Report, t time.Time) error {
	sid := r.GetSessionID()
	if _, ok := m.reports[sid]; !ok {
		m.reports[sid] = make([]Report, 0)
	}

	m.reports[sid] = append(m.reports[sid], r)
	return nil
}

func (m *mockTimeseriesDB) WriteDescription(d SessionDescription, t time.Time) error {
	sid := d.SessionID
	if _, ok := m.descs[sid]; !ok {
		m.descs[sid] = make([]SessionDescription, 0)
	}
	m.descs[sid] = append(m.descs[sid], d)
	return nil
}

func (m *mockTimeseriesDB) ReadReportRange(time.Time, time.Time) ([]Report, error) {
	reports := make([]Report, 0)
	for _, reps := range m.reports {
		reports = append(reports, reps...)
	}
	return reports, nil
}

func (m *mockTimeseriesDB) ReadSessionReports(sid string, start time.Time, end time.Time) (*ClientReport, *EndpointReport, error) {
	if reps, ok := m.reports[sid]; ok && len(reps) == 2 {
		_, ok := reps[0].(*ClientReport)
		if !ok {
			return reps[1].(*ClientReport), reps[0].(*EndpointReport), nil
		}
		return reps[0].(*ClientReport), reps[1].(*EndpointReport), nil
	}
	return nil, nil, fmt.Errorf("Failed to find session reports")
}

func (m *mockTimeseriesDB) ReadEndpointSessions(string, time.Time, time.Time) ([]SessionDescription, error) {
	return nil, nil
}

func (m *mockTimeseriesDB) ReadContentSessions(string, time.Time, time.Time) ([]SessionDescription, error) {
	return nil, nil
}

// InfluxTimeseriesDB implements TimeseriesDB using InfluxDB2
type InfluxTimeseriesDB struct {
	client         influxdb2.Client
	sessionsCtx    context.Context
	reportsCtx     context.Context
	sessionsWriter influxAPI.WriteAPIBlocking
	reportsWriter  influxAPI.WriteAPIBlocking
	dbReader       influxAPI.QueryAPI
	finder         infra.DataIndexReader
}

/*
NewInfluxTimeseriesDB creates a new instance of InfluxTimeseriesDB pointing
at the provided dbURL influx database authenticated with dbToken
*/
func NewInfluxTimeseriesDB(dbURL, dbToken string, finder infra.DataIndexReader) *InfluxTimeseriesDB {
	client := influxdb2.NewClient(dbURL, dbToken)

	return &InfluxTimeseriesDB{
		client:         client,
		sessionsCtx:    context.Background(),
		reportsCtx:     context.Background(),
		sessionsWriter: client.WriteAPIBlocking(OrganizationName, SessionsBucket),
		reportsWriter:  client.WriteAPIBlocking(OrganizationName, ReportsBucket),
		dbReader:       client.QueryAPI(OrganizationName),
		finder:         finder,
	}
}

// WriteDescription writes a session description to the SessionsBucket
func (ts *InfluxTimeseriesDB) WriteDescription(desc SessionDescription, t time.Time) error {
	url, err := ts.finder.GetContentID(desc.FunctionalID)
	if err != nil {
		return fmt.Errorf("Failed to associate functional id with url %s: %w", desc.FunctionalID, err)
	}

	tags := map[string]string{
		sessionIDTag:  desc.SessionID,
		contentIDTag:  url,
		endpointIDTag: desc.EndpointIdentity,
	}
	fields := map[string]interface{}{
		functionalIDField: desc.FunctionalID,
		clientIPField:     desc.ClientIP,
		endpointIPField:   desc.EndpointIP,
		bytesRecvField:    desc.BytesRecv,
		bytesNeededField:  desc.BytesNeeded,
		reportsAgreeField: desc.Agree,
	}

	entry := influxdb2.NewPoint("session", tags, fields, t.UTC())
	if err := ts.sessionsWriter.WritePoint(ts.sessionsCtx, entry); err != nil {
		return fmt.Errorf("Failed to write session entry: %w", err)
	}
	return nil
}

// WriteReport writes a Report to the InfluxDB ReportsBucket
func (ts *InfluxTimeseriesDB) WriteReport(r Report, t time.Time) error {
	url, err := ts.finder.GetContentID(r.GetFunctionalID())
	if err != nil {
		return fmt.Errorf("Failed to associate functional id with url %s: %w", r.GetFunctionalID(), err)
	}

	tags := map[string]string{
		sessionIDTag: r.GetSessionID(),
		contentIDTag: url,
	}
	fields := map[string]interface{}{
		functionalIDField: r.GetFunctionalID(),
		reportIPField:     r.GetIP(),
	}

	var measureType string
	switch report := r.(type) {
	case *ClientReport:
		measureType = clientReportMeasure
		fields[bytesRecvField] = report.BytesRecv
		fields[bytesNeededField] = report.BytesNeeded
		break
	case *EndpointReport:
		measureType = endpointReportMeasure
		fields[bytesServedField] = report.BytesServed
		tags[endpointIDTag] = report.Identity
	}

	entry := influxdb2.NewPoint(measureType, tags, fields, t.UTC())
	if err := ts.reportsWriter.WritePoint(ts.reportsCtx, entry); err != nil {
		return fmt.Errorf("Failed to write report entry: %w", err)
	}
	return nil
}

func (ts *InfluxTimeseriesDB) readReports(query string) ([]Report, error) {
	// Make Query
	result, err := ts.dbReader.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("Failed to read report range: %w", err)
	}

	// Consolidate report entry values
	reportBuilderMap := make(map[string]map[string]interface{})
	for result.Next() {
		record := result.Record()
		measurement := record.Measurement()
		sessionID := record.ValueByKey(sessionIDTag).(string)
		key := measurement + ":" + sessionID

		reportValues, ok := reportBuilderMap[key]
		if !ok {
			reportValues = map[string]interface{}{
				"type":       measurement,
				sessionIDTag: sessionID,
				contentIDTag: record.ValueByKey(contentIDTag).(string),
			}
			if measurement == endpointReportMeasure {
				reportValues[endpointIDTag] = record.ValueByKey(endpointIDTag).(string)
			}
			reportBuilderMap[key] = reportValues
		}
		reportValues[record.Field()] = record.Value()
	}

	// Build Report structures
	reports := make([]Report, 0, len(reportBuilderMap))
	var report Report
	for _, reportValues := range reportBuilderMap {
		switch reportValues["type"] {
		case clientReportMeasure:
			report = &ClientReport{
				SessionID:    reportValues[sessionIDTag].(string),
				FunctionalID: reportValues[functionalIDField].(string),
				ContentID:    reportValues[contentIDTag].(string),
				IP:           reportValues[reportIPField].(string),
				BytesRecv:    reportValues[bytesRecvField].(int64),
				BytesNeeded:  reportValues[bytesNeededField].(int64),
			}
			break
		case endpointReportMeasure:
			report = &EndpointReport{
				SessionID:    reportValues[sessionIDTag].(string),
				FunctionalID: reportValues[functionalIDField].(string),
				ContentID:    reportValues[contentIDTag].(string),
				IP:           reportValues[reportIPField].(string),
				Identity:     reportValues[endpointIDTag].(string),
				BytesServed:  reportValues[bytesServedField].(int64),
			}
		}

		reports = append(reports, report)
	}
	return reports, nil
}

func (ts *InfluxTimeseriesDB) readSessions(query string) ([]SessionDescription, error) {
	// Make Query
	result, err := ts.dbReader.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("Failed to read report range: %w", err)
	}

	// Consolidate session entry values
	sessionBuilderMap := make(map[string]map[string]interface{})
	for result.Next() {
		record := result.Record()
		sessionID := record.ValueByKey(sessionIDTag).(string)

		sessionValues, ok := sessionBuilderMap[sessionID]
		if !ok {
			sessionValues = map[string]interface{}{
				sessionIDTag:  sessionID,
				endpointIDTag: record.ValueByKey(endpointIDTag).(string),
				contentIDTag:  record.ValueByKey(contentIDTag).(string),
			}
			sessionBuilderMap[sessionID] = sessionValues
		}
		sessionValues[record.Field()] = record.Value()
	}

	// Build SessionDescriptions
	sessions := make([]SessionDescription, 0, len(sessionBuilderMap))
	for _, sessionValues := range sessionBuilderMap {
		session := SessionDescription{
			SessionID:        sessionValues[sessionIDTag].(string),
			FunctionalID:     sessionValues[functionalIDField].(string),
			ContentID:        sessionValues[contentIDTag].(string),
			ClientIP:         sessionValues[clientIPField].(string),
			EndpointIP:       sessionValues[endpointIPField].(string),
			EndpointIdentity: sessionValues[endpointIDTag].(string),
			BytesRecv:        sessionValues[bytesRecvField].(int64),
			BytesNeeded:      sessionValues[bytesNeededField].(int64),
			Agree:            sessionValues[reportsAgreeField].(bool),
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}

/*
ReadReportRange reads all data points from ReportsBucket in the specified time range.
Note that all times are converted to UTC time before queries are built
*/
func (ts *InfluxTimeseriesDB) ReadReportRange(start time.Time, end time.Time) ([]Report, error) {
	startStr := start.UTC().Format(time.RFC3339)
	endStr := end.UTC().Format(time.RFC3339)
	query := fmt.Sprintf(`from(bucket:"%s")|> range(start: %s, stop: %s)`, ReportsBucket, startStr, endStr)
	return ts.readReports(query)
}

// ReadSessionReports reads all data points from ReportsBucket that have a session_id that matched 'sid'
func (ts *InfluxTimeseriesDB) ReadSessionReports(sid string, start time.Time, end time.Time) (*ClientReport, *EndpointReport, error) {
	// Create query
	startStr := start.UTC().Format(time.RFC3339)
	endStr := end.UTC().Format(time.RFC3339)
	query := fmt.Sprintf(`from(bucket:"%s")|> range(start: %s, stop: %s) |> filter(fn: (r) => r["%s"] == "%s")`,
		ReportsBucket, startStr, endStr, sessionIDTag, sid)

	// Read reports
	reports, err := ts.readReports(query)
	if err != nil {
		return nil, nil, err
	} else if len(reports) != 2 {
		return nil, nil, fmt.Errorf("Invalid number of session reports: %d", len(reports))
	}

	// Return reports
	var clientReport *ClientReport
	var endpointReport *EndpointReport
	switch report := reports[0].(type) {
	case *ClientReport:
		clientReport = report
		endpointReport = reports[1].(*EndpointReport)
		break
	case *EndpointReport:
		clientReport = reports[1].(*ClientReport)
		endpointReport = report
	}
	return clientReport, endpointReport, nil
}

// ReadEndpointSessions reads all data points from SessionsBucket that are tagged with endpoint_id 'eid'
func (ts *InfluxTimeseriesDB) ReadEndpointSessions(eid string, start time.Time, end time.Time) ([]SessionDescription, error) {
	startStr := start.UTC().Format(time.RFC3339)
	endStr := end.UTC().Format(time.RFC3339)
	query := fmt.Sprintf(`from(bucket:"%s")|> range(start: %s, stop: %s) |> filter(fn: (r) => r["%s"] == "%s")`,
		SessionsBucket, startStr, endStr, endpointIDTag, eid)

	return ts.readSessions(query)
}

// ReadContentSessions reads all data points from SessionsBucket that are tagged with content_id 'cid'
func (ts *InfluxTimeseriesDB) ReadContentSessions(cid string, start time.Time, end time.Time) ([]SessionDescription, error) {
	startStr := start.UTC().Format(time.RFC3339)
	endStr := end.UTC().Format(time.RFC3339)
	query := fmt.Sprintf(`from(bucket:"%s")|> range(start: %s, stop: %s) |> filter(fn: (r) => r["%s"] == "%s")`,
		SessionsBucket, startStr, endStr, contentIDTag, cid)

	return ts.readSessions(query)
}

// Close closes the InfluxDB database connection
func (ts *InfluxTimeseriesDB) Close() {
	ts.client.Close()
}
