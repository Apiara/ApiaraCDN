package levi

import (
	"bytes"
	"io"
	"log"
	"net/http"
)

// copy 'req' body and query parameters into a new request directed at 'url'
func createReverseProxyRequest(req *http.Request, url string) (*http.Request, error) {
	body := bytes.NewBuffer(nil)
	if _, err := io.Copy(body, req.Body); err != nil {
		return nil, err
	}

	copyRequest, err := http.NewRequest(req.Method, url, body)
	if err != nil {
		return nil, err
	}

	copyRequest.URL.RawQuery = req.URL.Query().Encode()
	return copyRequest, nil
}

// create an http handler that forwards requests to 'internalURL' and returns result
func createProxyHandler(client *http.Client, internalURL string) func(http.ResponseWriter, *http.Request) {
	return func(resp http.ResponseWriter, req *http.Request) {
		// Create duplicate request for internal service
		proxyReq, err := createReverseProxyRequest(req, internalURL)
		if err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Perform internal API request
		proxyResp, err := client.Do(proxyReq)
		if err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Copy response to original requester
		if _, err := io.Copy(resp, proxyResp.Request.Body); err != nil {
			log.Println(err)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}
		resp.WriteHeader(proxyResp.StatusCode)
	}
}
