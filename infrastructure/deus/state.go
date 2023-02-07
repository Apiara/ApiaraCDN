package deus

const (
	RedisContentToServerListPrefix = "deus:location:cid:"
	RedisContentPullStatusPrefix   = "deus:dynamic:cid:sid:"
)

// Reader sub-interface for ContentLocationIndex
type ContentLocationIndexReader interface {
	IsContentServedByServer(cid string, serverID string) (bool, error)
	ContentServerList(cid string) ([]string, error)
	IsContentBeingServed(cid string) (bool, error)
	WasContentPulled(cid string, serverID string) (bool, error)
}

// Writer sub-interface for ContentLocationIndex
type ContentLocationIndexWriter interface {
	CreateContentLocationEntry(cid string, serverID string, dynamic bool) error
	DeleteContentLocationEntry(cid string, serverID string) error
}

/*
ContentLocationIndex allows changing/viewing of what content is being served on
what session servers in the network
*/
type ContentLocationIndex interface {
	ContentLocationIndexReader
	ContentLocationIndexWriter
}

// mockContentState is a mock implementation for testing
type mockContentLocationIndex struct {
	serveSet map[string]struct{}
}

func (m *mockContentLocationIndex) CreateContentLocationEntry(cid string, server string, dyn bool) error {
	m.serveSet[cid+server] = struct{}{}
	return nil
}

func (m *mockContentLocationIndex) DeleteContentLocationEntry(cid string, server string) error {
	delete(m.serveSet, cid+server)
	return nil
}

func (m *mockContentLocationIndex) IsContentBeingServed(cid string) (bool, error) {
	return false, nil
}

func (m *mockContentLocationIndex) ContentServerList(cid string) ([]string, error) {
	return nil, nil
}

func (m *mockContentLocationIndex) IsContentServedByServer(cid string, server string) (bool, error) {
	_, ok := m.serveSet[cid+server]
	return ok, nil
}

func (m *mockContentLocationIndex) WasContentPulled(string, string) (bool, error) { return true, nil }
