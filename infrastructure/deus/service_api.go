package deus

import (
	"log"
	"net/http"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

func handleStaleReport(cid string, checker DataValidator,
	locIndex ContentLocationIndex, manager ContentManager) {

	// Check report validity
	stale, err := checker.IsStale(cid)
	if err != nil {
		log.Printf("Failed to check stale status of %s: %v\n", cid, err)
		return
	}
	if !stale {
		return
	}

	// Find all affected servers
	dynamic := make(map[string]bool)
	serverList, err := locIndex.ContentServerList(cid)
	if err != nil {
		log.Printf("Failed to lookup list of servers serving %s: %v\n", cid, err)
		return
	}

	// Remove stale data
	manager.Lock()
	defer manager.Unlock()
	for _, serverID := range serverList {
		dynamic[serverID], err = locIndex.WasContentPulled(cid, serverID)
		if err != nil {
			log.Printf("Failed to lookup if %s was dynamically set to server %s: %v\n", cid, serverID, err)
		} else if err = manager.Remove(cid, serverID, dynamic[serverID]); err != nil {
			log.Printf("Failed to remove %s from server %s: %v\n", cid, serverID, err)
		}
	}

	// Re-process removed content
	for _, serverID := range serverList {
		if err = manager.Serve(cid, serverID, dynamic[serverID]); err != nil {
			locIndex.DeleteContentLocationEntry(cid, serverID)
			log.Printf("Failed to re-add %s to server %s: %v\n", cid, serverID, err)
		}
	}
}

// StartServiceAPI starts the API used for changing of network state during runtime
func StartServiceAPI(listenAddr string, checker DataValidator, locIndex ContentLocationIndex,
	decider PullDecider, manager ContentManager) {
	serviceAPI := http.NewServeMux()

	// Push allows manually pushing of data onto the network
	serviceAPI.HandleFunc(infra.DeusServiceAPIPushResource,
		func(resp http.ResponseWriter, req *http.Request) {
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			serverID := req.URL.Query().Get(infra.ServerIDHeader)

			manager.Lock()
			defer manager.Unlock()
			if err := manager.Serve(cid, serverID, false); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Purge allows manually purging of data from the network
	serviceAPI.HandleFunc(infra.DeusServiceAPIPurgeResource,
		func(resp http.ResponseWriter, req *http.Request) {
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			serverID := req.URL.Query().Get(infra.ServerIDHeader)

			manager.Lock()
			defer manager.Unlock()
			if err := manager.Remove(cid, serverID, false); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Stale Report invokes a check+remediation if the stated content is in a stale state
	serviceAPI.HandleFunc(infra.DeusServiceAPIStaleReportResource,
		func(resp http.ResponseWriter, req *http.Request) {
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			go handleStaleReport(cid, checker, locIndex, manager)
		})

	// Decider update update the pull decider with a new request
	serviceAPI.HandleFunc(infra.DeusServiceAPIPullDeciderResource,
		func(resp http.ResponseWriter, req *http.Request) {
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			serverID := req.URL.Query().Get(infra.ServerIDHeader)
			if err := decider.NewRequest(cid, serverID); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	log.Fatal(http.ListenAndServe(listenAddr, serviceAPI))
}
