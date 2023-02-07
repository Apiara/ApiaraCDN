package reiko

import (
	"log"
	"net/http"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

const (
	ContentRuleHeader = "content_rule"
)

/*
StartServiceAPI starts the API used checking whether a content id
is valid and can be served by the network
*/
func StartServiceAPI(listenAddr string, ruleset ContentRules) {
	serviceAPI := http.NewServeMux()

	// Validate content
	serviceAPI.HandleFunc(infra.ReikoServiceAPIValidateResource,
		func(resp http.ResponseWriter, req *http.Request) {
			cid := req.URL.Query().Get(infra.ContentIDHeader)

			matchFound, err := ruleset.DoesContentMatchRule(cid)
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
	serviceAPI.HandleFunc(infra.ReikoServiceAPIAddRuleResource,
		func(resp http.ResponseWriter, req *http.Request) {
			rule := req.URL.Query().Get(ContentRuleHeader)

			if err := ruleset.CreateContentPullRule(rule); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	// Remove content rule
	serviceAPI.HandleFunc(infra.ReikoServiceAPIDelRuleResource,
		func(resp http.ResponseWriter, req *http.Request) {
			rule := req.URL.Query().Get(ContentRuleHeader)

			if err := ruleset.DeleteContentPullRule(rule); err != nil {
				resp.WriteHeader(http.StatusInternalServerError)
				log.Println(err)
			}
		})

	log.Fatal(http.ListenAndServe(listenAddr, serviceAPI))
}
