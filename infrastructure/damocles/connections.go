package damocles

import (
	"fmt"
	"sync"
)

/*
ConnectionManager represents an object that can keep a list of different
websocket connections under different category names
*/
type ConnectionManager interface {
	CreateCategory(string) error
	DelCategory(string) error
	Put(string, Websocket) error
	Pop(string) (Websocket, error)
}

// mockConnectionManager is a testing mock for ConnectionManager
type mockConnectionManager struct{}

func (m *mockConnectionManager) CreateCategory(string) error { return nil }
func (m *mockConnectionManager) DelCategory(string) error    { return nil }
func (m *mockConnectionManager) Put(string, Websocket) error { return nil }
func (m *mockConnectionManager) Pop(string) (Websocket, error) {
	return &mockWebsocket{}, nil
}

// thread safe list of websockets with add+remove methods
type connectionList struct {
	mutex *sync.Mutex
	list  []Websocket
}

// add a connection to the end of the list
func (c *connectionList) addConnection(conn Websocket) {
	c.mutex.Lock()
	c.list = append(c.list, conn)
	c.mutex.Unlock()
}

// gets oldest connection while clearing inactive connection out of the list
func (c *connectionList) getOldestActiveConnection() (Websocket, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i := 0; i < len(c.list); i++ {
		conn := c.list[i]
		if conn.IsActive() {
			c.list[i] = nil // avoid memory leak
			c.list = c.list[i+1:]
			return conn, nil
		}
	}
	c.list = make([]Websocket, 0)
	return nil, fmt.Errorf("No active connections")
}

// EndpointConnectionManager implements ConnectionManager using connectionLists
type EndpointConnectionManager struct {
	mutex   *sync.RWMutex
	servers map[string]*connectionList
}

// NewEndpointConnectionManager returns a *EndpointConnectionManager
func NewEndpointConnectionManager() *EndpointConnectionManager {
	return &EndpointConnectionManager{
		mutex:   &sync.RWMutex{},
		servers: make(map[string]*connectionList),
	}
}

// CreateCategory creates a new connection category
func (m *EndpointConnectionManager) CreateCategory(id string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.servers[id]; exists {
		return fmt.Errorf("Failed to add connection category %s. Already exists", id)
	}
	m.servers[id] = &connectionList{
		mutex: &sync.Mutex{},
		list:  make([]Websocket, 0),
	}

	return nil
}

// DelCategory deletes an existing connection category
func (m *EndpointConnectionManager) DelCategory(id string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.servers[id]; !exists {
		return fmt.Errorf("Failed to remove connection category %s. Doesn't exist", id)
	}
	delete(m.servers, id)

	return nil
}

// Put adds a connection under the id category
func (m *EndpointConnectionManager) Put(id string, conn Websocket) error {
	m.mutex.RLock()
	connections, exists := m.servers[id]
	m.mutex.RUnlock()
	if !exists {
		return fmt.Errorf("Failed to add connection to %s. Category %s does not exist", id, id)
	}

	connections.addConnection(conn)
	return nil
}

// Pop returns the oldest active connection in the id endpoint category
func (m *EndpointConnectionManager) Pop(id string) (Websocket, error) {
	m.mutex.RLock()
	connections, exists := m.servers[id]
	m.mutex.RUnlock()
	if !exists {
		return nil, fmt.Errorf("Failed to pop connections from %s. Category %s doesn't exist", id, id)
	}

	return connections.getOldestActiveConnection()
}
