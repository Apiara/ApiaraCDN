package damocles

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

func handleAPIRequest(upgrader websocket.Upgrader, handler func(Websocket) error) func(http.ResponseWriter, *http.Request) {
	return func(resp http.ResponseWriter, req *http.Request) {
		conn, err := upgrader.Upgrade(resp, req, nil)
		if err != nil {
			log.Println(err)
			return
		}

		ws := NewGorillaWebsocket(conn)
		if err = handler(ws); err != nil {
			log.Println(err)
		}
	}
}

/*
StartSignalingAPI starts API for clients to ask for a signaling partner
and endpoints to be put in a signaling job queue
*/
func StartSignalingAPI(listenAddr string, servicer ClientServicer, allocator EndpointAllocator) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
		EnableCompression: true,
		CheckOrigin: func(r *http.Request) bool {
			// Add check with valid client origins for client API requests
			return true
		},
	}

	signalingAPI := http.NewServeMux()
	signalingAPI.HandleFunc("/client/match", handleAPIRequest(upgrader, servicer.MatchAndSignal))
	signalingAPI.HandleFunc("/endpoint/place", handleAPIRequest(upgrader, allocator.PlaceEndpoint))
	log.Fatal(http.ListenAndServe(listenAddr, signalingAPI))
}
