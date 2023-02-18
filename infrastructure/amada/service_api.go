package amada

import (
	"log"
	"net/http"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

// StartServiceAPI starts the API used for changing of network state during runtime
func StartServiceAPI(listenAddr string, servers state.ServerStateWriter, geoFinder IPGeoFinder) {
	serviceAPI := http.NewServeMux()

	// Set regional server address
	serviceAPI.HandleFunc(infra.AmadaServiceAPISetRegionResource,
		func(resp http.ResponseWriter, req *http.Request) {
			query := req.URL.Query()
			serverID := query.Get(infra.RegionServerIDHeader)
			publicAddr := query.Get(infra.ServerPublicAddrHeader)
			privateAddr := query.Get(infra.ServerPrivateAddrHeader)

			// Check if serverID is valid
			validServerID := false
			regionNames := geoFinder.RegionList()
			for i := 0; i < len(regionNames) && !validServerID; i++ {
				if serverID == regionNames[i] {
					validServerID = true
				}
			}

			// Create server entry
			if validServerID {
				if err := servers.CreateServerEntry(serverID, publicAddr, privateAddr); err != nil {
					resp.WriteHeader(http.StatusInternalServerError)
					log.Println(err)
				}
			} else {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Printf("failed to set addresses(%s, %s) for server(%s): invalid region mapping", publicAddr, privateAddr, serverID)
			}
		})

	// Remove regional server address
	serviceAPI.HandleFunc(infra.AmadaServiceAPIDelRegionResource,
		func(resp http.ResponseWriter, req *http.Request) {
			serverID := req.URL.Query().Get(infra.RegionServerIDHeader)

			if err := servers.DeleteServerEntry(serverID); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Update MaxMindDB File
	serviceAPI.HandleFunc(infra.AmadaServiceAPIUpdateGeoResource,
		func(resp http.ResponseWriter, req *http.Request) {
			fname := req.URL.Query().Get(infra.MMDBFileNameHeader)

			if err := geoFinder.LoadDatabase(fname); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	log.Fatal(http.ListenAndServe(listenAddr, serviceAPI))
}
