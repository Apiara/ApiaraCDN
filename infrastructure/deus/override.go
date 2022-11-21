package deus

import (
  "log"
  "net/http"
)

// StartOverrideAPI starts the API used for PUSH based content delivery(as opposed to PULL)
func StartOverrideAPI(listenAddr string, manager ContentManager) {
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
  log.Fatal(http.ListenAndServe(listenAddr, overrideAPI))
}
