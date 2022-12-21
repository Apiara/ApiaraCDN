package dominique

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

const (
	// PostgresRemediationTableNames is the table used by PostgresRemediationQueue
	PostgresRemediationTableName = "remediation_queue"
)

/*
ManualRemediationQueue represents an object that can queue up reports that failed to
be remediated for manual remediation by a human
*/
type ManualRemediationQueue interface {
	Write(client *ClientReport, endpoint *EndpointReport) error
	Close() error
}

// mock implementation of ManualRemediationQueue for testing purposes
type mockManualRemediationQueue struct{}

func (m *mockManualRemediationQueue) Write(*ClientReport, *EndpointReport) error { return nil }
func (m *mockManualRemediationQueue) Close() error                               { return nil }

/*
PostgresRemediationQueue implements ManualRemediationQueue using a postgresql
database as the storage system. The postgres table storing the remediation
tickets has the following schema:

CREATE TABLE remediation_queue (

	session_id TEXT PRIMARY KEY,
	functional_id TEXT,
	content_id TEXT,
	client_ip TEXT,
	endpoint_ip TEXT,
	endpoint_id TEXT,
	client_bytes_recv INT,
	client_bytes_needed INT,
	endpoint_bytes_served INT

)
*/
type PostgresRemediationQueue struct {
	db *sql.DB
}

/*
NewPostgresRemediationQueue creates a new PostgresRemediationQueue using the specified
database connection parameters passed in
*/
func NewPostgresRemediationQueue(host string, port int, user string,
	pass string, dbname string) (*PostgresRemediationQueue, error) {

	// Open database connection
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s "+
		"dbname=%s sslmode=disable", host, port, user, pass, dbname)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("Failed to open postgres remediation queue: %w", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("Failed to open postgres remediation queue: %w", err)
	}

	return &PostgresRemediationQueue{db}, nil
}

// Write stores the relevant information from the reports into the postgres remediation queue
func (p *PostgresRemediationQueue) Write(client *ClientReport, endpoint *EndpointReport) error {
	insertStatement := "INSERT INTO " + PostgresRemediationTableName + "(session_id, functional_id, " +
		"content_id, client_ip, endpoint_ip, endpoint_id, client_bytes_recv, client_bytes_needed, " +
		"endpoint_bytes_served) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	_, err := p.db.Exec(insertStatement, client.SessionID, client.FunctionalID, client.ContentID, client.IP,
		endpoint.IP, endpoint.Identity, client.BytesRecv, client.BytesNeeded, endpoint.BytesServed)

	if err != nil {
		return fmt.Errorf("Failed to queue reports for remediation: %w", err)
	}
	return nil
}

// Close closes the underlying postgresql database connection
func (p *PostgresRemediationQueue) Close() error {
	return p.db.Close()
}
