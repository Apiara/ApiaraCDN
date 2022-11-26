package damocles

import (
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// Websocket represens a websocket connection acccording to RFC6455
type Websocket interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	Close() error
	SetReadDeadline(time.Time) error
	IsActive() bool
}

// mockWebsocket is a testing mock for Websocket
type mockWebsocket struct {
	msgs       [][]byte
	writeCount int
}

func (m *mockWebsocket) ReadMessage() (int, []byte, error) {
	if len(m.msgs) != 0 {
		msg := m.msgs[0]
		m.msgs = m.msgs[1:]
		return websocket.TextMessage, msg, nil
	}
	return -1, nil, fmt.Errorf("No message")
}

func (m *mockWebsocket) WriteMessage(int, []byte) error  { m.writeCount++; return nil }
func (m *mockWebsocket) Close() error                    { return nil }
func (m *mockWebsocket) SetReadDeadline(time.Time) error { return nil }
func (m *mockWebsocket) IsActive() bool                  { return len(m.msgs) != 0 }

type websocketMsg struct {
	dataType int
	data     []byte
}

// GorillaWebsocket implements Websocket using the gorilla websocket library
type GorillaWebsocket struct {
	buffer []websocketMsg
	conn   *websocket.Conn
}

// NewGorillaWebsocket creates a new GorillaWebsocket with the returned *websocket.conn
func NewGorillaWebsocket(conn *websocket.Conn) *GorillaWebsocket {
	return &GorillaWebsocket{
		buffer: make([]websocketMsg, 0),
		conn:   conn,
	}
}

/*
ReadMessage attempts to read unreturned messages from the message bufffer.
If buffer is empty, forwards the call to websocket.conn.ReadMessage()
*/
func (g *GorillaWebsocket) ReadMessage() (int, []byte, error) {
	if len(g.buffer) > 0 {
		msg := g.buffer[0]
		g.buffer = g.buffer[1:]
		return msg.dataType, msg.data, nil
	}
	return g.conn.ReadMessage()
}

// WriteMessage forwards to websocket.conn.WriteMessage()
func (g *GorillaWebsocket) WriteMessage(msgType int, data []byte) error {
	return g.conn.WriteMessage(msgType, data)
}

// Close forwards to websocket.conn.Close()
func (g *GorillaWebsocket) Close() error {
	return g.conn.Close()
}

// SetReadDeadline forwards to websocket.conn.SetReadDeadline()
func (g *GorillaWebsocket) SetReadDeadline(t time.Time) error {
	return g.conn.SetReadDeadline(t)
}

/*
IsActive checks if the connection is still active by attempting to read a
message. If successful, the message is put in the buffer for later retrieval
and true is returned. Otherwise, false is returned
*/
func (g *GorillaWebsocket) IsActive() bool {
	msgType, data, err := g.conn.ReadMessage()
	if err != nil {
		return false
	}
	g.buffer = append(g.buffer, websocketMsg{msgType, data})
	return true
}
