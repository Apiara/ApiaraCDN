package dominique

import (
	"fmt"
)

var (
	// conflictErr is returned when a client and endpoint report can't be reconciled
	conflictErr error = fmt.Errorf("failed due to conflicting session reports")
)

// Report represents a session report from a client or endpoint
type Report interface {
	GetSessionID() string
	GetFunctionalID() string
	GetIP() string
}

// ClientReport implements a report sent by a client
type ClientReport struct {
	SessionID    string `json:"SessionID"`
	FunctionalID string `json:"FunctionalID"`
	IP           string `json:"IP"`
	BytesRecv    int64  `json:"BytesRecv"`
	BytesNeeded  int64  `json:"BytesServed"`
}

func (c *ClientReport) GetSessionID() string    { return c.SessionID }
func (c *ClientReport) GetFunctionalID() string { return c.FunctionalID }
func (c *ClientReport) GetIP() string           { return c.IP }

// EndpointReport implements a report sent by a client
type EndpointReport struct {
	SessionID    string `json:"SessionID"`
	FunctionalID string `json:"FunctionalID"`
	IP           string `json:"IP"`
	Identity     string `json:"Identity"`
	BytesServed  int64  `json:"BytesServed"`
}

func (e *EndpointReport) GetSessionID() string    { return e.SessionID }
func (e *EndpointReport) GetFunctionalID() string { return e.FunctionalID }
func (e *EndpointReport) GetIP() string           { return e.IP }

/*
SessionDescription represents a holistic view of a session
as agreed upon by both the client and the endpoint
*/
type SessionDescription struct {
	SessionID        string
	FunctionalID     string
	ClientIP         string
	EndpointIP       string
	EndpointIdentity string
	BytesRecv        int64
	BytesNeeded      int64
	Agree            bool
}
