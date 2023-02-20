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

// RequestIPExtractor represents a function that can extract the source IP of a http request
type RequestIPExtractor func(req *http.Request) (string, error)

// ExtractRequestIP is a production implementation of RequestIPExtractor
func ExtractRequestIP(req *http.Request) (string, error) {
	ip, _, err := net.SplitHostPort(req.RemoteAddr)
	return ip, err
}

/*
DebuggingExtractRequestIP is a implementation of RequestIPExtractor that allows
explicit specification of what the assumed IP should be in the http request in order
to allow for testing and debugging
*/
func DebuggingExtractRequestIP(req *http.Request) (string, error) {
	ip := req.URL.Query().Get(infra.DebugModeForcedRequestIPParam)
	return ip, nil
}

// Returns a region and address of edge server for a request based on it's IP
func matchReqToRegionalServer(req *http.Request, extractIP RequestIPExtractor,
	geoFinder IPGeoFinder, serverIndex state.ServerStateReader) (string, string, error) {
	ip, err := extractIP(req)
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

// Forwards a request to Deus PullDecider
func sendNewRequestUpdate(addr string, cid string, region string) error {
	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		return err
	}

	query := url.Values{}
	query.Add(infra.ContentIDParam, cid)
	query.Add(infra.RegionServerIDParam, region)
	req.URL.RawQuery = query.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received unsuccessful http response: %d", resp.StatusCode)
	}
	return nil
}

// Response type for all routing API requests
type RouteResponse struct {
	SessionServerRegion string `json:"region"`
	SessionServerAddr   string `json:"address"`
}

/*
StartDeviceRoutingAPI starts the API used by clients and endpoints to find
region based session servers
*/
func StartDeviceRoutingAPI(listenAddr string, extractor RequestIPExtractor, geoFinder IPGeoFinder,
	dataState state.ContentLocationStateReader, serverIndex state.ServerStateReader,
	deciderAPIAddr string) {

	routeAPI := http.NewServeMux()
	routeAPI.HandleFunc(infra.AmadaRouteAPIClientResource,
		func(resp http.ResponseWriter, req *http.Request) {
			// Lookup local session server for client region
			region, serverAddr, err := matchReqToRegionalServer(req, extractor, geoFinder, serverIndex)
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
				return
			}

			// Forward request to Pull Decider
			cid := req.URL.Query().Get(infra.ContentIDParam)
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
			region, serverAddr, err := matchReqToRegionalServer(req, extractor, geoFinder, serverIndex)
			if err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
				return
			}
			fmt.Printf("Got request. Mapped to region(%s) with address(%s)\n", region, serverAddr)

			// Return endpoints regional session server
			if err = json.NewEncoder(resp).Encode(RouteResponse{region, serverAddr}); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
			}
		})
	log.Fatal(http.ListenAndServe(listenAddr, routeAPI))
}
