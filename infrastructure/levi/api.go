package levi

import (
	"log"
	"net/http"
	"net/url"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

func StartReportAPI(listenAddr, staleAPI, reportAPI string) {
	// Create internal resource URLs
	staleReportURL, err := url.JoinPath(staleAPI, infra.DeusServiceAPIStaleReportResource)
	if err != nil {
		log.Fatal(err)
	}
	endpointReportURL, err := url.JoinPath(reportAPI, infra.DominiqueReportAPIEndpointResource)
	if err != nil {
		log.Fatal(err)
	}
	clientReportURL, err := url.JoinPath(reportAPI, infra.DominiqueReportAPIClientResource)
	if err != nil {
		log.Fatal(err)
	}

	// Create request handler
	reportMux := http.NewServeMux()
	client := http.DefaultClient
	reportMux.HandleFunc(infra.LeviReportAPIStaleResource, createProxyHandler(client, staleReportURL))
	reportMux.HandleFunc(infra.LeviReportAPIEndpointSessionResource, createProxyHandler(client, endpointReportURL))
	reportMux.HandleFunc(infra.LeviReportAPIClientSessionResource, createProxyHandler(client, clientReportURL))

	// Start server
	log.Fatal(http.ListenAndServe(listenAddr, reportMux))
}

func StartContentAPI(listenAddr, ruleAPI, contentAPI string) {
	// Create internal resource paths
	ruleAddURL, err := url.JoinPath(ruleAPI, infra.ReikoServiceAPIAddRuleResource)
	if err != nil {
		log.Fatal(err)
	}
	ruleDelURL, err := url.JoinPath(ruleAPI, infra.ReikoServiceAPIDelRuleResource)
	if err != nil {
		log.Fatal(err)
	}
	contentPushURL, err := url.JoinPath(contentAPI, infra.DeusServiceAPIPushResource)
	if err != nil {
		log.Fatal(err)
	}
	contentPurgeURL, err := url.JoinPath(contentAPI, infra.DeusServiceAPIPurgeResource)
	if err != nil {
		log.Fatal(err)
	}

	// Create request handler
	contentMux := http.NewServeMux()
	client := http.DefaultClient
	contentMux.HandleFunc(infra.LeviContentAPIPushAddResource, createProxyHandler(client, contentPushURL))
	contentMux.HandleFunc(infra.LeviContentAPIPushRemoveResource, createProxyHandler(client, contentPurgeURL))
	contentMux.HandleFunc(infra.LeviContentAPIPullAddResource, createProxyHandler(client, ruleAddURL))
	contentMux.HandleFunc(infra.LeviContentAPIPullRemoveResource, createProxyHandler(client, ruleDelURL))

	// Start server
	log.Fatal(http.ListenAndServe(listenAddr, contentMux))
}

func StartDataAccessAPI(listenAddr, dataAPI string) {
	// Create internal resource paths
	fetchDataURL, err := url.JoinPath(dataAPI, infra.DominiqueDataAPIFetchResource)
	if err != nil {
		log.Fatal(err)
	}

	// Create request handler
	dataAccessMux := http.NewServeMux()
	client := http.DefaultClient
	dataAccessMux.HandleFunc(infra.LeviDataAPIFetchResource, createProxyHandler(client, fetchDataURL))

	// Start server
	log.Fatal(http.ListenAndServe(listenAddr, dataAccessMux))
}
