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

// StartDamoclesServiceAPI starts the API used to modify service state
func StartDamoclesServiceAPI(listenAddr string, updater CategoryUpdater) {
	serviceAPI := http.NewServeMux()
	serviceAPI.HandleFunc("/category/add", func(resp http.ResponseWriter, req *http.Request) {
		id := req.URL.Query().Get(infra.FunctionalIDHeader)
		if err := updater.CreateCategory(id); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
		}
	})
	serviceAPI.HandleFunc("/category/del", func(resp http.ResponseWriter, req *http.Request) {
		id := req.URL.Query().Get(infra.FunctionalIDHeader)
		if err := updater.DelCategory(id); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
		}
	})
	log.Fatal(http.ListenAndServe(listenAddr, serviceAPI))
}
