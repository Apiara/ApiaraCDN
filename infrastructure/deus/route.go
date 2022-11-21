package deus

import (
  "net/http"
  "net"
  "log"
  "encoding/json"
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

// StartDeviceRoutingAPI starts the API used by clients and endpoints to find
// region based session servers
func StartDeviceRoutingAPI(geoFinder IPGeoFinder, dataState ContentState,
  serverIndex GeoServerIndex, decider PullDecider, listenAddr string) {

  //Response type for all routing API requests
  type RouteResponse struct {
    SessionServerAddr string `json:"SessionServerAddr"`
  }

  routeAPI := http.NewServeMux()
  routeAPI.HandleFunc("/client", func(resp http.ResponseWriter, req *http.Request) {
    // Lookup local session server for client region
    serverAddr, err := matchReqToRegionalServer(req, geoFinder, serverIndex)
    if err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
      return
    }

    // Forward request to Pull Decider
    cid := req.URL.Query().Get(ContentIDHeader)
    decider.NewRequest(cid, serverAddr)

    // Check if requested content is being served and respond appropriately
    serving, err := dataState.IsBeingServed(cid, serverAddr)
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

  routeAPI.HandleFunc("/endpoint", func(resp http.ResponseWriter, req *http.Request) {
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
