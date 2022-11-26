package dominique

import (
  "time"
  "testing"
)

func TestInfluxTimeseriesDB(t *testing.T) {
  dbURL := "http://localhost:8086"
  dbToken := "tDJfcuaQ1jUCbQrH_nktJyTpjgN8-EUwj7-HQ0sMxFHdmcRQjASya7vj3doeqKW1F6QlGu76Fa0uMjHWrD5v6A=="
  finder := &mockURLIndex{}

  timeseries := NewInfluxTimeseriesDB(dbURL, dbToken, finder)

  // Test WriteDescription
  desc := SessionDescription{
    SessionID: "session",
    FunctionalID: "fid",
    ClientIP: "ip",
    EndpointIP: "ip",
    EndpointIdentity: "id",
    BytesRecv: 1024,
    BytesNeeded: 1024,
    Agree: true,
  }

  if err := timeseries.WriteDescription(time.Now(), desc); err != nil {
    t.Fatalf("Failed to write description: %v", err)
  }

  // Test WriteReport client
  cReport := ClientReport{
    SessionID: "session",
    FunctionalID: "fid",
    IP: "ip",
    BytesRecv: 1024,
    BytesNeeded: 1024,
  }

  if err := timeseries.WriteReport(time.Now(), &cReport); err != nil {
    t.Fatalf("Failed to write report: %v", err)
  }

  // Test WriteReport Endpoint
  eReport := EndpointReport{
    SessionID: "session",
    FunctionalID: "fid",
    IP: "ip",
    BytesServed: 1024,
    Identity: "id",
  }

  if err := timeseries.WriteReport(time.Now(), &eReport); err != nil {
    t.Fatalf("Failed to write report: %v", err)
  }
}
