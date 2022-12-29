package amada

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
func StartServiceAPI(listenAddr string, servers GeoServerIndex, geoFinder IPGeoFinder) {
	serviceAPI := http.NewServeMux()

	// Set regional server address
	serviceAPI.HandleFunc(infra.AmadaServiceAPISetRegionResource,
		func(resp http.ResponseWriter, req *http.Request) {
			region := req.URL.Query().Get(RegionNameHeader)
			serverID := req.URL.Query().Get(ServerIDHeader)

			if err := servers.SetRegionAddress(region, serverID); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Remove regional server address
	serviceAPI.HandleFunc(infra.AmadaServiceAPIDelRegionResource,
		func(resp http.ResponseWriter, req *http.Request) {
			region := req.URL.Query().Get(RegionNameHeader)

			if err := servers.RemoveRegionAddress(region); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Update MaxMindDB File
	serviceAPI.HandleFunc(infra.AmadaServiceAPIUpdateGeoResource,
		func(resp http.ResponseWriter, req *http.Request) {
			fname := req.URL.Query().Get(MMDBFileNameHeader)

			if err := geoFinder.LoadDatabase(fname); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	log.Fatal(http.ListenAndServe(listenAddr, serviceAPI))
}
