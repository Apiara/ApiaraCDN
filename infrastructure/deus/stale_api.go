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

	// Remove stale data
	dynamic := make(map[string]bool)
	serverList, err := locIndex.ServerList(cid)
	if err != nil {
		log.Printf("Failed to lookup list of servers serving %s: %v\n", cid, err)
		return
	}
	for _, serverID := range serverList {
		dynamic[serverID], err = locIndex.WasDynamicallySet(cid, serverID)
		if err != nil {
			log.Printf("Failed to lookup if %s was dynamically set to server %s: %v\n", cid, serverID, err)
		} else if err = manager.Remove(cid, serverID, dynamic[serverID]); err != nil {
			log.Printf("Failed to remove %s from server %s: %v\n", cid, serverID, err)
		}
	}

	// Re-process removed content
	for _, serverID := range serverList {
		if err = manager.Serve(cid, serverID, dynamic[serverID]); err != nil {
			locIndex.Remove(cid, serverID)
			log.Printf("Failed to re-add %s to server %s: %v\n", cid, serverID, err)
		}
	}
}

func StartStaleReadReportAPI(listenAddr string, checker DataValidator,
	locIndex ContentLocationIndex, manager ContentManager) {

	staleAPI := http.NewServeMux()
	staleAPI.HandleFunc(infra.DeusStaleReportAPIResource,
		func(resp http.ResponseWriter, req *http.Request) {
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			go handleStaleReport(cid, checker, locIndex, manager)
		})
	log.Fatal(http.ListenAndServe(listenAddr, staleAPI))
}
