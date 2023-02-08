package crow

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

type allocationResponse struct {
	ServeList []string `json:"serve"`
}

/*
StartDataAllocatorAPI starts the API service for endpoints to
be allocated data to serve on the network
*/
func StartDataAllocatorAPI(listenAddr string, allocator LocationAwareDataAllocator) {
	allocateAPI := http.NewServeMux()
	allocateAPI.HandleFunc(infra.CrowAllocateAPIResource, func(resp http.ResponseWriter, req *http.Request) {
		location := req.URL.Query().Get(infra.LocationHeader)
		bytesStr := req.URL.Query().Get(infra.ByteSizeHeader)
		availableSpace, err := strconv.ParseInt(bytesStr, 10, 64)
		if err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}

		serveList, err := allocator.AllocateSpace(location, availableSpace)
		if err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}

		response := allocationResponse{serveList}
		if err = json.NewEncoder(resp).Encode(&response); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
		}
	})
}
