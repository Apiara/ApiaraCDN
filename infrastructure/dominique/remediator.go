package dominique

/*
Remediator represents an object that implements a remediation strategy that
tries to create a consistent SessionDescription based on the Client and
Endpoint reports
*/
type Remediator interface {
	Reconcile(*ClientReport, *EndpointReport) (SessionDescription, error)
}

/*
TimeframeRemediator implements Remediator and simply checks if the reports
are an exact match. This handles the case where a report being sent gets
delayed for some reason, causing a failure to match within the timeframe
the SessionProcessor acts in.
*/
type TimeframeRemediator struct{}

func NewTimeframeRemediator() *TimeframeRemediator {
	return &TimeframeRemediator{}
}

func (r *TimeframeRemediator) Reconcile(client *ClientReport, endpoint *EndpointReport) (SessionDescription, error) {
	return createSessionDescription(client, endpoint)
}

/*
ByteOffsetRemediator implements Remediator and checks if the difference
between client.BytesRecv and endpoint.BytesServed is minimal enough
to be automatically remediated.
*/
type ByteOffsetRemediator struct {
	offsetAllowed int64
}

func NewByteOffsetRemediator(offsetAllowed int64) *ByteOffsetRemediator {
	return &ByteOffsetRemediator{offsetAllowed}
}

func (r *ByteOffsetRemediator) Reconcile(client *ClientReport, endpoint *EndpointReport) (SessionDescription, error) {
	// Check for unreconcilable conflicts
	if client.SessionID != endpoint.SessionID || client.FunctionalID != endpoint.FunctionalID {
		return SessionDescription{}, conflictErr
	}

	// Check if byte offset is within acceptable range
	byteOffset := endpoint.BytesServed - client.BytesRecv
	if byteOffset < 0 {
		byteOffset *= -1
	}
	if byteOffset > r.offsetAllowed {
		return SessionDescription{}, conflictErr
	}

	// Return description with endpoint.BytesServed
	return SessionDescription{
		SessionID:        client.SessionID,
		FunctionalID:     client.FunctionalID,
		ClientIP:         client.IP,
		EndpointIP:       endpoint.IP,
		EndpointIdentity: endpoint.Identity,
		BytesRecv:        endpoint.BytesServed,
		BytesNeeded:      client.BytesNeeded,
		Agree:            false,
	}, nil
}
