package deus

import (
  "log"
  "net/http"
  infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

const (
  MMDBFileNameHeader = "mmdb"
  RegionNameHeader = "region_id"
  ServerIDHeader = "server_id"
)

// StartOverrideAPI starts the API used for changing of network state during runtime
func StartOverrideAPI(listenAddr string, manager ContentManager, servers GeoServerIndex, geoFinder IPGeoFinder) {
  overrideAPI := http.NewServeMux()

  // Push allows manually pushing of data onto the network
  overrideAPI.HandleFunc("/push", func(resp http.ResponseWriter, req *http.Request) {
    cid := req.URL.Query().Get(infra.ContentIDHeader)
    serverID := req.URL.Query().Get(ServerIDHeader)

    if err := manager.Serve(cid, serverID, false); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  // Purge allows manually purging of data from the network
  overrideAPI.HandleFunc("/purge", func(resp http.ResponseWriter, req *http.Request) {
    cid := req.URL.Query().Get(infra.ContentIDHeader)
    serverID := req.URL.Query().Get(ServerIDHeader)

    if err := manager.Remove(cid, serverID, false); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  // Set regional server address
  overrideAPI.HandleFunc("/setRegion", func(resp http.ResponseWriter, req *http.Request) {
    region := req.URL.Query().Get(RegionNameHeader)
    serverID := req.URL.Query().Get(ServerIDHeader)

    if err := servers.SetRegionAddress(region, serverID); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  // Remove regional server address
  overrideAPI.HandleFunc("/removeRegion", func(resp http.ResponseWriter, req *http.Request) {
    region := req.URL.Query().Get(RegionNameHeader)

    if err := servers.RemoveRegionAddress(region); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  // Update MaxMindDB File
  overrideAPI.HandleFunc("/updateIPDB", func(resp http.ResponseWriter, req *http.Request) {
    fname := req.URL.Query().Get(MMDBFileNameHeader)

    if err := geoFinder.LoadDatabase(fname); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  log.Fatal(http.ListenAndServe(listenAddr, overrideAPI))
}
