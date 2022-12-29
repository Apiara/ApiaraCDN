package amada

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/deus"
)

func matchReqToRegionalServer(req *http.Request, geoFinder IPGeoFinder,
	serverIndex GeoServerIndex) (string, error) {
	ip, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return "", err
	}
	loc, err := geoFinder.Location(ip)
	if err != nil {
		return "", err
	}

	return serverIndex.GetAddress(loc)
}

func sendNewRequestUpdate(addr string, cid string, serverAddr string) error {
	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		return err
	}

	query := url.Values{}
	query.Add(infra.ContentIDHeader, cid)
	query.Add(infra.ServerIDHeader, serverAddr)
	req.URL.RawQuery = query.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Received unsuccessful http response: %d", resp.StatusCode)
	}
	return nil
}

/*
StartDeviceRoutingAPI starts the API used by clients and endpoints to find
region based session servers
*/
func StartDeviceRoutingAPI(listenAddr string, geoFinder IPGeoFinder,
	dataState deus.ContentLocationIndexReader, serverIndex GeoServerIndex, deciderAPIAddr string) {

	//Response type for all routing API requests
	type RouteResponse struct {
		SessionServerAddr string `json:"SessionServerAddr"`
	}

	routeAPI := http.NewServeMux()
	routeAPI.HandleFunc(infra.AmadaRouteAPIClientResource,
		func(resp http.ResponseWriter, req *http.Request) {
			// Lookup local session server for client region
			serverAddr, err := matchReqToRegionalServer(req, geoFinder, serverIndex)
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
				return
			}

			// Forward request to Pull Decider
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			err = sendNewRequestUpdate(deciderAPIAddr, cid, serverAddr)
			if err != nil {
				log.Printf("Request for %s was not ingested by decider: %v", cid, err)
			}

			// Check if requested content is being served and respond appropriately
			serving, err := dataState.IsServedByServer(cid, serverAddr)
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
				return
			}

			if serving {
				// Return clients regional session server
				if err = json.NewEncoder(resp).Encode(RouteResponse{serverAddr}); err != nil {
					resp.WriteHeader(http.StatusInternalServerError)
					log.Println(err)
				}
			} else {
				resp.WriteHeader(http.StatusNoContent)
			}
		})

	routeAPI.HandleFunc(infra.AmadaRouteAPIEndpointResource,
		func(resp http.ResponseWriter, req *http.Request) {
			// Lookup regional server for endpoint region
			serverAddr, err := matchReqToRegionalServer(req, geoFinder, serverIndex)
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
				return
			}

			// Return endpoints regional session server
			if err = json.NewEncoder(resp).Encode(RouteResponse{serverAddr}); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
			}
		})
	log.Fatal(http.ListenAndServe(listenAddr, routeAPI))
}
