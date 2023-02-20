package damocles

import (
	"log"
	"net/http"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

/*
CategoryUpdater represents an object that can update internal state objects
such as ConnectionManager and NeedTracker with updated category information
*/
type CategoryUpdater interface {
	CreateCategory(string) error
	DelCategory(string) error
}

// StartServiceAPI starts the API used to modify service state
func StartServiceAPI(listenAddr string, updater CategoryUpdater) {
	serviceAPI := http.NewServeMux()
	serviceAPI.HandleFunc(infra.DamoclesServiceAPIAddResource,
		func(resp http.ResponseWriter, req *http.Request) {
			id := req.URL.Query().Get(infra.ContentFunctionalIDParam)
			if err := updater.CreateCategory(id); err != nil {
				log.Println(err)
				resp.WriteHeader(http.StatusInternalServerError)
			}
		})
	serviceAPI.HandleFunc(infra.DamoclesServiceAPIDelResource,
		func(resp http.ResponseWriter, req *http.Request) {
			id := req.URL.Query().Get(infra.ContentFunctionalIDParam)
			if err := updater.DelCategory(id); err != nil {
				log.Println(err)
				resp.WriteHeader(http.StatusInternalServerError)
			}
		})
	log.Fatal(http.ListenAndServe(listenAddr, serviceAPI))
}
