package deus

import (
  "log"
  "net/http"
)

// StartOverrideAPI starts the API used for PUSH based content delivery(as opposed to PULL)
func StartOverrideAPI(listenAddr string, manager ContentManager,
  servers GeoServerIndex, ruleset ContentRules) {
  overrideAPI := http.NewServeMux()

  // Push allows manually pushing of data onto the network
  overrideAPI.HandleFunc("/push", func(resp http.ResponseWriter, req *http.Request) {
    cid := req.URL.Query().Get(ContentIDHeader)
    serverID := req.URL.Query().Get(ServerIDHeader)

    if err := manager.Serve(cid, serverID, false); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  // Purge allows manually purging of data from the network
  overrideAPI.HandleFunc("/purge", func(resp http.ResponseWriter, req *http.Request) {
    cid := req.URL.Query().Get(ContentIDHeader)
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

  // Set content rule
  overrideAPI.HandleFunc("/setContentRule", func(resp http.ResponseWriter, req *http.Request) {
    rule := req.URL.Query().Get(ContentRuleHeader)

    if err := ruleset.SetRule(rule); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  // Remove content rule
  overrideAPI.HandleFunc("/removeContentRule", func(resp http.ResponseWriter, req *http.Request) {
    rule := req.URL.Query().Get(ContentRuleHeader)

    if err := ruleset.DelRule(rule); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  log.Fatal(http.ListenAndServe(listenAddr, overrideAPI))
}
