package damocles

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/gorilla/websocket"
)

type allocateRequest struct {
	Serving []string `json:"serving"`
}

type allocateResponse struct {
	ChosenID string   `json:"allocation"`
	BadIDs   []string `json:"bad"`
}

/*
EndpointAllocator represents an object that can allocate an endpoint
to the proper job queue based on the content it is actively serving
*/
type EndpointAllocator interface {
	PlaceEndpoint(Websocket) error
}

/*
JSONEndpointAllocator implements EndpointAllocator
using JSON messages for communication
*/
type NeedEndpointAllocator struct {
	tracker     NeedTracker
	connections ConnectionManager
}

// PlaceEndpoint places the endpoint in a job queue based on content held
func (e *NeedEndpointAllocator) PlaceEndpoint(endpoint Websocket) error {
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
	badIDs := make([]string, 0)
	chosenID := ""
	maxNeed := int64(math.MinInt64)
	for _, id := range req.Serving {
		need, err := e.tracker.GetScore(id)
		if err != nil {
			badIDs = append(badIDs, id)
		} else if need > maxNeed {
			maxNeed = need
			chosenID = id
		}
	}
	if chosenID == "" {
		return fmt.Errorf("Failed to find valid allocation for endpoint. All IDs being served are bad")
	}

	// Respond
	resp := allocateResponse{
		ChosenID: chosenID,
		BadIDs:   badIDs,
	}
	data, err = json.Marshal(&resp)
	if err != nil {
		return fmt.Errorf("Failed to marshal endpoint allocate response: %w", err)
	}
	if err = endpoint.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("Failed to write allocate response to websocket: %w", err)
	}

	// Place in connection queue
	e.connections.Put(chosenID, endpoint)
	e.tracker.AddAllocation(chosenID)
	return nil
}
