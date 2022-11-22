package reiko

import (
  "log"
  "net/http"
  infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

const (
  ContentRuleHeader = "content_rule"
)

// StartOverrideAPI starts the API used for PUSH based content delivery(as opposed to PULL)
func StartRulesetAPI(listenAddr string, ruleset ContentRules) {
  rulesetAPI := http.NewServeMux()

  // Validate content
  rulesetAPI.HandleFunc("/validate", func(resp http.ResponseWriter, req *http.Request) {
    cid := req.URL.Query().Get(infra.ContentIDHeader)

    matchFound, err := ruleset.MatchesRule(cid)
    if err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
      return
    }

    // If no match found, return error code. Else, default to 200 OK :)
    if !matchFound {
      resp.WriteHeader(http.StatusNotAcceptable)
    }
  })

  // Set content rule
  rulesetAPI.HandleFunc("/setContentRule", func(resp http.ResponseWriter, req *http.Request) {
    rule := req.URL.Query().Get(ContentRuleHeader)

    if err := ruleset.SetRule(rule); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  // Remove content rule
  rulesetAPI.HandleFunc("/removeContentRule", func(resp http.ResponseWriter, req *http.Request) {
    rule := req.URL.Query().Get(ContentRuleHeader)

    if err := ruleset.DelRule(rule); err != nil {
      resp.WriteHeader(http.StatusInternalServerError)
      log.Println(err)
    }
  })

  log.Fatal(http.ListenAndServe(listenAddr, rulesetAPI))
}
