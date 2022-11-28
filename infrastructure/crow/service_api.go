package crow

import (
	"log"
	"net/http"
	"strconv"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

/*
StartServiceAPI starts the API that informs the service of what content
to start or stop allocating to endpoints
*/
func StartServiceAPI(listenAddr string, allocator DataAllocator) {
	serviceAPI := http.NewServeMux()

	serviceAPI.HandleFunc(infra.CrowServiceAPIPublishResource, func(resp http.ResponseWriter, req *http.Request) {
		fid := req.URL.Query().Get(infra.FunctionalIDHeader)
		sizeStr := req.URL.Query().Get(infra.ByteSizeHeader)

		byteSize, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err = allocator.NewEntry(fid, byteSize); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
		}
	})
	serviceAPI.HandleFunc(infra.CrowServiceAPIPurgeResource, func(resp http.ResponseWriter, req *http.Request) {
		fid := req.URL.Query().Get(infra.FunctionalIDHeader)

		if err := allocator.DelEntry(fid); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
		}
	})
	http.ListenAndServe(listenAddr, serviceAPI)
}
