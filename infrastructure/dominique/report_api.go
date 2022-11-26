package dominique

import (
	"encoding/json"
	"log"
	"net/http"
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
	reportingAPI.HandleFunc("/report/client", reportHandler(getEmptyClientReport, matcher))
	reportingAPI.HandleFunc("/report/client", reportHandler(getEmptyEndpointReport, matcher))
	log.Fatal(http.ListenAndServe(listenAddr, reportingAPI))
}
