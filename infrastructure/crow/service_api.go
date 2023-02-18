package crow

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

/*
StartServiceAPI starts the API that informs the service of what content
to start or stop allocating to endpoints
*/
func StartServiceAPI(listenAddr string, allocator LocationAwareDataAllocator) {
	serviceAPI := http.NewServeMux()

	serviceAPI.HandleFunc(infra.CrowServiceAPIPublishResource, func(resp http.ResponseWriter, req *http.Request) {
		location := req.URL.Query().Get(infra.LocationHeader)
		fid := req.URL.Query().Get(infra.ContentFunctionalIDHeader)
		sizeStr := req.URL.Query().Get(infra.ByteSizeHeader)

		byteSize, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err = allocator.NewEntry(location, fid, byteSize); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
		}
	})
	serviceAPI.HandleFunc(infra.CrowServiceAPIPurgeResource, func(resp http.ResponseWriter, req *http.Request) {
		location := req.URL.Query().Get(infra.LocationHeader)
		fid := req.URL.Query().Get(infra.ContentFunctionalIDHeader)

		if err := allocator.DelEntry(location, fid); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
		}
	})
	fmt.Println("Listening on " + listenAddr)
	http.ListenAndServe(listenAddr, serviceAPI)
}
