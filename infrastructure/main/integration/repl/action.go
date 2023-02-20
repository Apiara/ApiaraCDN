package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

const (
	PushCommand        = "push"
	PurgeCommand       = "purge"
	SetRuleCommand     = "srule"
	UnsetRuleCommand   = "urule"
	RouteCommand       = "route"
	AllocateCommand    = "allocate"
	SetRegionCommand   = "sregion"
	UnsetRegionCommand = "uregion"

	ExitCommand = "exit"
	HelpCommand = "help"

	ClientRouteParam   = "client"
	EndpointRouteParam = "endpoint"

	SuccessResult = "Success"
)

type action func(args []string) (string, error)

func makeHTTPRequest(url string, query url.Values, body io.Reader, client *http.Client, result interface{}) error {
	// Create HTTP request
	req, err := http.NewRequest(http.MethodGet, url, body)
	if err != nil {
		return err
	}
	req.URL.RawQuery = query.Encode()

	// Perform request and check for failures
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad HTTP status: %s", resp.Status)
	}

	// Copy body into buffer
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return err
	}

	// Unmarshal response body into result using gob
	if buf.Len() > 0 {
		dec := gob.NewDecoder(&buf)
		if err = dec.Decode(result); err != nil {
			return err
		}
	}
	return nil
}

func createActionMap(conf replConfig) map[string]action {
	localhost := "http://127.0.0.1:"
	pushResource := localhost + strconv.Itoa(conf.ContentManagerPort) + infra.DeusServiceAPIPushResource
	purgeResource := localhost + strconv.Itoa(conf.ContentManagerPort) + infra.DeusServiceAPIPurgeResource
	setRuleResource := localhost + strconv.Itoa(conf.RuleManagerPort) + infra.ReikoServiceAPIAddRuleResource
	unsetRuleResource := localhost + strconv.Itoa(conf.RuleManagerPort) + infra.ReikoServiceAPIDelRuleResource
	clientLocateResource := localhost + strconv.Itoa(conf.RoutePort) + infra.AmadaRouteAPIClientResource
	endpointLocateResource := localhost + strconv.Itoa(conf.RoutePort) + infra.AmadaRouteAPIEndpointResource
	allocateResource := localhost + strconv.Itoa(conf.AllocatorPort) + infra.CrowAllocateAPIResource
	setRegionResource := localhost + strconv.Itoa(conf.RegionManagerPort) + infra.AmadaServiceAPISetRegionResource
	unsetRegionResource := localhost + strconv.Itoa(conf.RegionManagerPort) + infra.AmadaServiceAPIDelRegionResource

	actions := make(map[string]action)
	actions[PushCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentIDHeader, args[0])
		query.Add(infra.RegionServerIDHeader, args[1])

		err := makeHTTPRequest(pushResource, query, nil, http.DefaultClient, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[PurgeCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentIDHeader, args[0])
		query.Add(infra.RegionServerIDHeader, args[1])

		err := makeHTTPRequest(purgeResource, query, nil, http.DefaultClient, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[SetRuleCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentRuleHeader, args[0])

		err := makeHTTPRequest(setRuleResource, query, nil, http.DefaultClient, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[UnsetRuleCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentRuleHeader, args[0])

		err := makeHTTPRequest(unsetRuleResource, query, nil, http.DefaultClient, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[RouteCommand] = func(args []string) (string, error) {
		var resourceUrl string
		var result string
		query := url.Values{}

		switch strings.ToLower(args[0]) {
		case ClientRouteParam:
			resourceUrl = clientLocateResource
			query.Add(infra.ContentIDHeader, args[1])
		case EndpointRouteParam:
			resourceUrl = endpointLocateResource
		}

		err := makeHTTPRequest(resourceUrl, query, nil, http.DefaultClient, &result)
		if err != nil {
			return "", err
		}
		return result, nil
	}
	actions[AllocateCommand] = func(args []string) (string, error) {
		var result string
		query := url.Values{}
		query.Add(infra.RegionServerIDHeader, args[0])
		query.Add(infra.ByteSizeHeader, args[1])

		err := makeHTTPRequest(allocateResource, query, nil, http.DefaultClient, &result)
		if err != nil {
			return "", err
		}
		return result, nil
	}
	actions[SetRegionCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.RegionServerIDHeader, args[0])
		query.Add(infra.ServerPublicAddrHeader, args[1])
		query.Add(infra.ServerPrivateAddrHeader, args[2])

		err := makeHTTPRequest(setRegionResource, query, nil, http.DefaultClient, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[UnsetRegionCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.RegionServerIDHeader, args[0])

		err := makeHTTPRequest(unsetRegionResource, query, nil, http.DefaultClient, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[ExitCommand] = func([]string) (string, error) {
		os.Exit(0)
		return "", nil
	}
	actions[HelpCommand] = func([]string) (string, error) {
		return fmt.Sprint(
			"\tPUSH <content_id> <region_id>\n",
			"\tPURGE <content_id> <region_id>\n",
			"\tSRULE <rule>\n",
			"\tURULE <rule>\n",
			"\tROUTE CLIENT <content_id>\n",
			"\tROUTE ENDPOINT\n",
			"\tALLOCATE <region_id> <available_bytes>\n",
			"\tSREGION <region_id> <public_addr> <private_addr>\n",
			"\tUREGION <region_id>\n",
			"\tEXIT\n",
		), nil
	}
	return actions
}
