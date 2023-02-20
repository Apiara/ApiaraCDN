package damocles

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gorilla/websocket"
)

// request format client sends to server
type clientRequest struct {
	FunctionalID string `json:"FunctionalID"`
}

// response format to initiate signaling to client and endpoint
type signalingResponse struct {
	Signal bool `json:"Signal"`
}

/*
ClientServicer represents an object that can service a clients
matching and signaling request
*/
type ClientServicer interface {
	MatchAndSignal(Websocket) error
}

/*
NeedClientServicer implements ClientServicer in a system where a NeedTracker
is used to allocate endpoints to different content delivery job queues. This
requires the NeedTracker to keep track of how many client requests for
each data ID their are
*/
type NeedClientServicer struct {
	connections ConnectionManager
	tracker     NeedTracker
}

// NewNeedClientServicer returns a NeedClientServicer
func NewNeedClientServicer(connStore ConnectionManager, tracker NeedTracker) *NeedClientServicer {
	return &NeedClientServicer{
		connections: connStore,
		tracker:     tracker,
	}
}

/*
MatchAndSignal finds an endpoint with the appropriate data for client
and performs signaling to assist in the establishing of a p2p connection
*/
func (c *NeedClientServicer) MatchAndSignal(client Websocket) error {
	// Receive content request
	_, data, err := client.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read client message: %w", err)
	}
	req := clientRequest{}
	if err = json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("failed to unmarshal client message: %w", err)
	}
	id := req.FunctionalID
	c.tracker.AddRequest(id)

	// Find endpoint match
	endpoint, err := c.connections.Pop(id)
	if err != nil {
		failedMsg := signalingResponse{Signal: false}
		data, jsonErr := json.Marshal(failedMsg)
		if jsonErr != nil {
			log.Println(jsonErr)
		} else {
			client.WriteMessage(websocket.TextMessage, data)
		}
		client.Close()
		return fmt.Errorf("failed to retrieve endpoint for client: %w", err)
	}

	// Send signaling start message to both client and endpoint
	startMsg := signalingResponse{Signal: true}
	data, err = json.Marshal(startMsg)
	if err != nil {
		client.Close()
		endpoint.Close()
		return fmt.Errorf("failed to marshal signal start response: %w", err)
	}
	if err = client.WriteMessage(websocket.TextMessage, data); err != nil {
		client.Close()
		endpoint.Close()
		return fmt.Errorf("failed to send message to client: %w", err)
	}
	if err = endpoint.WriteMessage(websocket.TextMessage, data); err != nil {
		client.Close()
		endpoint.Close()
		return fmt.Errorf("failed to send message to endpoint: %w", err)
	}

	// Start signaling session
	go signal(client, endpoint)
	return nil
}
