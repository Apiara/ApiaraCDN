package damocles

import (
	"encoding/json"
	"fmt"
)

type allocateRequest struct {
	Serving []string `json:"serving"`
}

type EndpointAllocator interface {
	PlaceEndpoint(Websocket) error
}

type JSONEndpointAllocator struct {
	connections ConnectionManager
}

func (e *JSONEndpointAllocator) PlaceEndpoint(endpoint Websocket) error {
	// Read allocate request from endpoint
	_, data, err := endpoint.ReadMessage()
	if err != nil {
		return fmt.Errorf("Failed to read endpoint message: %w", err)
	}

	req := allocateRequest{}
	if err = json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("Failed to unmarshal endpoint allocate request: %w", err)
	}

	// Choose serving FID to place

	// Place in connection queue

	return nil
}
