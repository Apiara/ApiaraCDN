package dominique

import (
  "fmt"
  "time"
  "context"
  "github.com/influxdata/influxdb-client-go/v2"
  influxAPI "github.com/influxdata/influxdb-client-go/v2/api"
)

const (
  // InfluxDB organization name
  OrganizationName = "dominique"

  // InfluxDB database for matched sessions
  SessionsBucket = "matched_sessions"
  //InfluxDB bucket for unmatched or conflicted reports
  ReportsBucket = "unmatched_reports"
)

/* TimeseriesDB represents an object that can store Reports
and SessionDescriptions as a timeseries of events */
type TimeseriesDB interface {
  WriteReport(t time.Time, r Report) error
  WriteDescription(t time.Time, desc SessionDescription) error
}

// InfluxTimeseriesDB implements TimeseriesDB using InfluxDB2
type InfluxTimeseriesDB struct {
  client influxdb2.Client
  sessionsCtx context.Context
  reportsCtx context.Context
  sessionsClient influxAPI.WriteAPIBlocking
  reportsClient influxAPI.WriteAPIBlocking
  finder URLIndex
}

/* NewInfluxTimeseriesDB creates a new instance of InfluxTimeseriesDB pointing
at the provided dbURL influx database authenticated with dbToken */
func NewInfluxTimeseriesDB(dbURL, dbToken string, finder URLIndex) *InfluxTimeseriesDB {
  client := influxdb2.NewClient(dbURL, dbToken)

  return &InfluxTimeseriesDB{
    client: client,
    sessionsCtx: context.Background(),
    reportsCtx: context.Background(),
    sessionsClient: client.WriteAPIBlocking(OrganizationName, SessionsBucket),
    reportsClient: client.WriteAPIBlocking(OrganizationName, ReportsBucket),
    finder: finder,
  }
}

// WriteDescription writes a session description to the SessionsBucket
func (ts *InfluxTimeseriesDB) WriteDescription(t time.Time, desc SessionDescription) error {
  url, err := ts.finder.FunctionalIDToURL(desc.FunctionalID)
  if err != nil {
    return fmt.Errorf("Failed to associate functional id with url %s: %w", desc.FunctionalID, err)
  }

  tags := map[string]string{
    "session": desc.SessionID,
    "url": url,
    "endpoint_id": desc.EndpointIdentity,
  }
  fields := map[string]interface{}{
    "fid": desc.FunctionalID,
    "client_ip": desc.ClientIP,
    "endpoint_ip": desc.EndpointIP,
    "bytes_recv": desc.BytesRecv,
    "bytes_needed": desc.BytesNeeded,
    "agree": desc.Agree,
  }

  entry := influxdb2.NewPoint("session", tags, fields, t)
  if err := ts.sessionsClient.WritePoint(ts.sessionsCtx, entry); err != nil {
    return fmt.Errorf("Failed to write session entry: %w", err)
  }
  return nil
}

// WriteReport writes a Report to the InfluxDB ReportsBucket
func (ts *InfluxTimeseriesDB) WriteReport(t time.Time, r Report) error {
  url, err := ts.finder.FunctionalIDToURL(r.GetFunctionalID())
  if err != nil {
    return fmt.Errorf("Failed to associate functional id with url %s: %w", r.GetFunctionalID(), err)
  }

  tags := map[string]string{
    "session": r.GetSessionID(),
    "url": url,
  }
  fields := map[string]interface{}{
    "fid": r.GetFunctionalID(),
    "ip": r.GetIP(),
  }

  var measureType string
  switch report := r.(type) {
  case *ClientReport:
    measureType = "client"
    fields["bytes_recv"] = report.BytesRecv
    fields["bytes_needed"] = report.BytesNeeded
    break
  case *EndpointReport:
    measureType = "endpoint"
    fields["bytes_served"] = report.BytesServed
    tags["identity"] = report.Identity
  }

  entry := influxdb2.NewPoint(measureType, tags, fields, t)
  if err := ts.reportsClient.WritePoint(ts.reportsCtx, entry); err != nil {
    return fmt.Errorf("Failed to write report entry: %w", err)
  }
  return nil
}

// Close closes the InfluxDB database connection
func (ts *InfluxTimeseriesDB) Close() {
  ts.client.Close()
}
