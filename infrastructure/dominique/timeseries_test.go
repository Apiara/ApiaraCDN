package dominique

import (
	"testing"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/state"
	"github.com/stretchr/testify/assert"
)

func TestInfluxDBTimeseriesDBReader(t *testing.T) {
	// Initialize database and resources
	dbURL := "http://localhost:8086"
	dbToken := "eNV46n5ythDdO-yyN0issEyksel1kD1yHoA7rINxfvF8RwX_6GOg_o6UoTXie6isf-Kqg_WT7lHFY-FDBMuIkw=="
	finder := state.NewMockMicroserviceState()
	finder.CreateContentEntry("read_cid", "read_fid", 2048, []string{})

	timeseries := NewInfluxTimeseriesDB(dbURL, dbToken, finder)

	// Test ReadEndpointSessions and ReadContentSessions
	desc := SessionDescription{
		SessionID:        "read_session",
		FunctionalID:     "read_fid",
		ContentID:        "read_cid",
		ClientIP:         "read_ip",
		EndpointIP:       "read_ip",
		EndpointIdentity: "read_identity",
		BytesRecv:        2048,
		BytesNeeded:      2048,
		Agree:            true,
	}

	writeTime := time.Now()
	if err := timeseries.WriteDescription(desc, writeTime); err != nil {
		t.Fatalf("Failed to write description for read testing: %v", err)
	}

	startRange := writeTime.Add(-5 * time.Second)
	endRange := writeTime.Add(5 * time.Second)
	descriptions, err := timeseries.ReadEndpointSessions("read_identity", startRange, endRange)
	if err != nil {
		t.Fatalf("Failed to read descriptions by endpoint ID: %v", err)
	}
	assert.Equal(t, 1, len(descriptions), "Wrong number of descriptions retrieved by identity")
	assert.Equal(t, desc, descriptions[0], "Read description doesn't match actual description")

	descriptions, err = timeseries.ReadContentSessions("read_cid", startRange, endRange)
	if err != nil {
		t.Fatalf("Failed to read descriptions by content ID: %v", err)
	}
	assert.Equal(t, 1, len(descriptions), "Wrong number of descriptions retrieved by content ID")
	assert.Equal(t, desc, descriptions[0], "Read description doesn't match actual description")

	// Test ReadReportRange and ReadSessionReports
	cReport := ClientReport{
		SessionID:    "read_session",
		FunctionalID: "read_fid",
		IP:           "read_ip",
		BytesRecv:    1024,
		BytesNeeded:  1024,
	}

	if err := timeseries.WriteReport(&cReport, writeTime); err != nil {
		t.Fatalf("Failed to write report: %v", err)
	}

	// Test WriteReport Endpoint
	eReport := EndpointReport{
		SessionID:    "read_session",
		FunctionalID: "read_fid",
		IP:           "read_ip",
		BytesServed:  1024,
		Identity:     "read_id",
	}

	if err := timeseries.WriteReport(&eReport, writeTime); err != nil {
		t.Fatalf("Failed to write report: %v", err)
	}

	reports, err := timeseries.ReadReportRange(startRange, endRange)
	if err != nil {
		t.Fatalf("Failed to read report range: %v", err)
	}
	assert.Equal(t, 2, len(reports), "Failed to read correct amount of reports")

	_, _, err = timeseries.ReadSessionReports("read_session", startRange, endRange)
	if err != nil {
		t.Fatalf("Failed to read report range by session id: %v", err)
	}
}

func TestInfluxTimeseriesDBWriter(t *testing.T) {
	dbURL := "http://localhost:8086"
	dbToken := "eNV46n5ythDdO-yyN0issEyksel1kD1yHoA7rINxfvF8RwX_6GOg_o6UoTXie6isf-Kqg_WT7lHFY-FDBMuIkw=="
	finder := state.NewMockMicroserviceState()
	finder.CreateContentEntry("url_fid", "fid", 1024, []string{})

	timeseries := NewInfluxTimeseriesDB(dbURL, dbToken, finder)

	// Test WriteDescription
	desc := SessionDescription{
		SessionID:        "session",
		FunctionalID:     "fid",
		ClientIP:         "ip",
		EndpointIP:       "ip",
		EndpointIdentity: "id",
		BytesRecv:        1024,
		BytesNeeded:      1024,
		Agree:            true,
	}

	if err := timeseries.WriteDescription(desc, time.Now()); err != nil {
		t.Fatalf("Failed to write description: %v", err)
	}

	// Test WriteReport client
	cReport := ClientReport{
		SessionID:    "session",
		FunctionalID: "fid",
		IP:           "ip",
		BytesRecv:    1024,
		BytesNeeded:  1024,
	}

	if err := timeseries.WriteReport(&cReport, time.Now()); err != nil {
		t.Fatalf("Failed to write report: %v", err)
	}

	// Test WriteReport Endpoint
	eReport := EndpointReport{
		SessionID:    "session",
		FunctionalID: "fid",
		IP:           "ip",
		BytesServed:  1024,
		Identity:     "id",
	}

	if err := timeseries.WriteReport(&eReport, time.Now()); err != nil {
		t.Fatalf("Failed to write report: %v", err)
	}
}
