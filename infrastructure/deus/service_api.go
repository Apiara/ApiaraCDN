package deus

import (
	"log"
	"net/http"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

const (
	MMDBFileNameHeader = "mmdb"
	RegionNameHeader   = "region_id"
	ServerIDHeader     = "server_id"
)

// StartServiceAPI starts the API used for changing of network state during runtime
func StartServiceAPI(listenAddr string, manager ContentManager, servers GeoServerIndex, geoFinder IPGeoFinder) {
	serviceAPI := http.NewServeMux()

	// Push allows manually pushing of data onto the network
	serviceAPI.HandleFunc(infra.DeusServiceAPIPushResource,
		func(resp http.ResponseWriter, req *http.Request) {
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			serverID := req.URL.Query().Get(ServerIDHeader)

			if err := manager.Serve(cid, serverID, false); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Purge allows manually purging of data from the network
	serviceAPI.HandleFunc(infra.DeusServiceAPIPurgeResource,
		func(resp http.ResponseWriter, req *http.Request) {
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			serverID := req.URL.Query().Get(ServerIDHeader)

			if err := manager.Remove(cid, serverID, false); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Set regional server address
	serviceAPI.HandleFunc(infra.DeusServiceAPISetRegionResource,
		func(resp http.ResponseWriter, req *http.Request) {
			region := req.URL.Query().Get(RegionNameHeader)
			serverID := req.URL.Query().Get(ServerIDHeader)

			if err := servers.SetRegionAddress(region, serverID); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Remove regional server address
	serviceAPI.HandleFunc(infra.DeusServiceAPIDelRegionResource,
		func(resp http.ResponseWriter, req *http.Request) {
			region := req.URL.Query().Get(RegionNameHeader)

			if err := servers.RemoveRegionAddress(region); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Update MaxMindDB File
	serviceAPI.HandleFunc(infra.DeusServiceAPIUpdateGeoResource,
		func(resp http.ResponseWriter, req *http.Request) {
			fname := req.URL.Query().Get(MMDBFileNameHeader)

			if err := geoFinder.LoadDatabase(fname); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	log.Fatal(http.ListenAndServe(listenAddr, serviceAPI))
}
