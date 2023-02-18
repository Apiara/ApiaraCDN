package amada

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

func matchReqToRegionalServer(req *http.Request, geoFinder IPGeoFinder,
	serverIndex state.ServerStateReader) (string, string, error) {
	ip, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return "", "", err
	}
	region, err := geoFinder.Location(ip)
	if err != nil {
		return "", "", err
	}

	serverAddr, err := serverIndex.GetServerPublicAddress(region)
	if err != nil {
		return "", "", err
	}

	return region, serverAddr, nil
}

func sendNewRequestUpdate(addr string, cid string, region string) error {
	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		return err
	}

	query := url.Values{}
	query.Add(infra.ContentIDHeader, cid)
	query.Add(infra.RegionServerIDHeader, region)
	req.URL.RawQuery = query.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received unsuccessful http response: %d", resp.StatusCode)
	}
	return nil
}

/*
StartDeviceRoutingAPI starts the API used by clients and endpoints to find
region based session servers
*/
func StartDeviceRoutingAPI(listenAddr string, geoFinder IPGeoFinder,
	dataState state.ContentLocationStateReader, serverIndex state.ServerStateReader,
	deciderAPIAddr string) {

	//Response type for all routing API requests
	type RouteResponse struct {
		SessionServerRegion string `json:"region"`
		SessionServerAddr   string `json:"address"`
	}

	routeAPI := http.NewServeMux()
	routeAPI.HandleFunc(infra.AmadaRouteAPIClientResource,
		func(resp http.ResponseWriter, req *http.Request) {
			// Lookup local session server for client region
			region, serverAddr, err := matchReqToRegionalServer(req, geoFinder, serverIndex)
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
				return
			}

			// Forward request to Pull Decider
			cid := req.URL.Query().Get(infra.ContentIDHeader)
			err = sendNewRequestUpdate(deciderAPIAddr, cid, region)
			if err != nil {
				log.Printf("Request for %s was not ingested by decider: %v", cid, err)
			}

			// Check if requested content is being served and respond appropriately
			serving, err := dataState.IsContentServedByServer(cid, region)
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
				return
			}

			if serving {
				// Return clients regional session server
				if err = json.NewEncoder(resp).Encode(RouteResponse{region, serverAddr}); err != nil {
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
			region, serverAddr, err := matchReqToRegionalServer(req, geoFinder, serverIndex)
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
				return
			}

			// Return endpoints regional session server
			if err = json.NewEncoder(resp).Encode(RouteResponse{region, serverAddr}); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
			}
		})
	log.Fatal(http.ListenAndServe(listenAddr, routeAPI))
}
