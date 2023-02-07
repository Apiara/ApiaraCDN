package amada

// GeoServerIndex provides lookup of session server for a particular region
type GeoServerIndex interface {
	GetRegionAddress(location string) (string, error)
	SetRegionAddress(location string, address string) error
	RemoveRegionAddress(location string) error
}

// mockGeoServerIndex is a mock implementation for testing
type mockGeoServerIndex struct{}

func (m *mockGeoServerIndex) GetRegionAddress(string) (string, error) { return "", nil }
func (m *mockGeoServerIndex) SetRegionAddress(string, string) error   { return nil }
func (m *mockGeoServerIndex) RemoveRegionAddress(string) error        { return nil }
