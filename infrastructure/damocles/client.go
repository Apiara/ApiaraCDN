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

// JSONClientServicer implements ClientServicer using JSON for communication
type JSONClientServicer struct {
	connections ConnectionManager
}

/*
MatchAndSignal find an endpoint with the appropriate data for client
and performs signaling to assist in the establishing of a p2p connection
*/
func (c *JSONClientServicer) MatchAndSignal(client Websocket) error {
	// Receive content request
	_, data, err := client.ReadMessage()
	if err != nil {
		return fmt.Errorf("Failed to read client message: %w", err)
	}
	req := clientRequest{}
	if err = json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("Failed to unmarshal client message: %w", err)
	}

	// Find endpoint match
	id := req.FunctionalID
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
		return fmt.Errorf("Failed to retrieve endpoint for client: %w", err)
	}

	// Send signaling start message to both client and endpoint
	startMsg := signalingResponse{Signal: true}
	data, err = json.Marshal(startMsg)
	if err != nil {
		client.Close()
		endpoint.Close()
		return fmt.Errorf("Failed to marshal signal start response: %w", err)
	}
	if err = client.WriteMessage(websocket.TextMessage, data); err != nil {
		client.Close()
		endpoint.Close()
		return fmt.Errorf("Failed to send message to client: %w", err)
	}
	if err = endpoint.WriteMessage(websocket.TextMessage, data); err != nil {
		client.Close()
		endpoint.Close()
		return fmt.Errorf("Failed to send message to endpoint: %w", err)
	}

	// Start signaling session
	go signal(client, endpoint)
	return nil
}
