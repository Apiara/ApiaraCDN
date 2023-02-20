package damocles

import (
	"time"
)

var (
	/*
	  Amount of time the signaling process will run
	  before timing out and ending the signaling session
	*/
	SignalingTimeout = time.Minute
)

// Performs reads on w and sends along returned channel. Closes channel on error
func websocketToChannel(w Websocket) <-chan websocketMsg {
	msgChan := make(chan websocketMsg)
	go func() {
		for {
			msgType, data, err := w.ReadMessage()
			if err != nil {
				close(msgChan)
				return
			}
			msgChan <- websocketMsg{msgType, data}
		}
	}()
	return msgChan
}

/*
signal passes messages between the client and endpoint websockets until
one connection closes or SignalingTimeout passes without a new message
*/
func signal(client Websocket, endpoint Websocket) {
	// Ensure connections close after signaling ends
	defer client.Close()
	defer endpoint.Close()

	// Create message readers and timeout
	clientMsgs := websocketToChannel(client)
	endpointMsgs := websocketToChannel(endpoint)
	timeout := time.After(SignalingTimeout)

	// Perform message passing
	var ok bool
	var msg websocketMsg
	for {
		select {
		case msg, ok = <-clientMsgs:
			if !ok {
				return
			}
			endpoint.WriteMessage(msg.dataType, msg.data)
		case msg, ok = <-endpointMsgs:
			if !ok {
				return
			}
			client.WriteMessage(msg.dataType, msg.data)
		case <-timeout:
			return
		}
	}
}
