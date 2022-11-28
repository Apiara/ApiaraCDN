package dominique

import (
	"encoding/json"
	"log"
	"net/http"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

// Helpers to pass to reportHandler
func getEmptyClientReport() Report   { return &ClientReport{} }
func getEmptyEndpointReport() Report { return &EndpointReport{} }

// Generate report handler for any 'Report' returned from createReport
func reportHandler(createReport func() Report, matcher SessionProcessor) func(resp http.ResponseWriter, req *http.Request) {
	return func(resp http.ResponseWriter, req *http.Request) {
		report := createReport()
		if err := json.NewDecoder(req.Body).Decode(report); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err := matcher.AddReport(report); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// StartReportingAPI starts the API for clients and endpoints to report sessions
func StartReportingAPI(listenAddr string, matcher SessionProcessor) {
	reportingAPI := http.NewServeMux()
	reportingAPI.HandleFunc(infra.DominiqueReportAPIClientResource,
		reportHandler(getEmptyClientReport, matcher))
	reportingAPI.HandleFunc(infra.DominiqueReportAPIEndpointResource,
		reportHandler(getEmptyEndpointReport, matcher))
	log.Fatal(http.ListenAndServe(listenAddr, reportingAPI))
}
