package dominique

type ManualRemediationQueue interface {
	Write(client *ClientReport, endpoint *EndpointReport) error
}

type mockManualRemediationQueue struct{}

func (m *mockManualRemediationQueue) Write(*ClientReport, *EndpointReport) error { return nil }

type PostgresRemediationQueue struct {
}
