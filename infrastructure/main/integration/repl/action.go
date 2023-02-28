package main

import (
	"bytes"
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
	ReportCommand      = "report"
	StatCommand        = "stat"

	ExitCommand = "exit"
	HelpCommand = "help"

	ClientCommandParam   = "client"
	EndpointCommandParam = "endpoint"

	SumStatCommandParam        = "sum"
	IncStatCommandParam        = "inc"
	UserKeyStatCommandParam    = "user"
	ContentKeyStatCommandParam = "content"

	SuccessResult = "Success"
)

type decoder func(*bytes.Buffer, interface{}) error

func stringDecoder(buf *bytes.Buffer, result interface{}) error {
	strResult := result.(*string)
	*strResult = buf.String()
	return nil
}

func makeHTTPRequest(url string, query url.Values, body io.Reader,
	client *http.Client, dec decoder, result interface{}) error {
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
		if err = dec(&buf, result); err != nil {
			return err
		}
	}
	return nil
}

type action func(args []string) (string, error)

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
	clientReportResource := localhost + strconv.Itoa(conf.ReportAPIPort) + infra.DominiqueReportAPIClientResource
	endpointReportResource := localhost + strconv.Itoa(conf.ReportAPIPort) + infra.DominiqueReportAPIEndpointResource
	statQueryResource := localhost + strconv.Itoa(conf.StatQueryPort) + infra.DominiqueDataAPIFetchResource

	actions := make(map[string]action)
	actions[PushCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentIDParam, args[0])
		query.Add(infra.RegionServerIDParam, args[1])

		err := makeHTTPRequest(pushResource, query, nil, http.DefaultClient, nil, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[PurgeCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentIDParam, args[0])
		query.Add(infra.RegionServerIDParam, args[1])

		err := makeHTTPRequest(purgeResource, query, nil, http.DefaultClient, nil, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[SetRuleCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentRuleParam, args[0])

		err := makeHTTPRequest(setRuleResource, query, nil, http.DefaultClient, nil, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[UnsetRuleCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentRuleParam, args[0])

		err := makeHTTPRequest(unsetRuleResource, query, nil, http.DefaultClient, nil, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[RouteCommand] = func(args []string) (string, error) {
		var resourceUrl string
		var result string
		query := url.Values{}
		query.Add(infra.DebugModeForcedRequestIPParam, args[1])

		switch strings.ToLower(args[0]) {
		case ClientCommandParam:
			resourceUrl = clientLocateResource
			query.Add(infra.ContentIDParam, args[2])
		case EndpointCommandParam:
			resourceUrl = endpointLocateResource
		}

		err := makeHTTPRequest(resourceUrl, query, nil, http.DefaultClient, stringDecoder, &result)
		if err != nil {
			return "", err
		}
		return result, nil
	}
	actions[AllocateCommand] = func(args []string) (string, error) {
		var result string
		query := url.Values{}
		query.Add(infra.RegionServerIDParam, args[0])
		query.Add(infra.ContentByteSizeParam, args[1])

		err := makeHTTPRequest(allocateResource, query, nil, http.DefaultClient, stringDecoder, &result)
		if err != nil {
			return "", err
		}
		return result, nil
	}
	actions[SetRegionCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.RegionServerIDParam, args[0])
		query.Add(infra.ServerPublicAddrParam, args[1])
		query.Add(infra.ServerPrivateAddrParam, args[2])

		err := makeHTTPRequest(setRegionResource, query, nil, http.DefaultClient, nil, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[UnsetRegionCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.RegionServerIDParam, args[0])

		err := makeHTTPRequest(unsetRegionResource, query, nil, http.DefaultClient, nil, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[ReportCommand] = func(args []string) (string, error) {
		if len(args) < 2 {
			return "", fmt.Errorf("not enough arguments")
		}
		report := strings.Join(args[1:], " ")
		body := bytes.NewBufferString(report)

		var err error
		switch strings.ToLower(args[0]) {
		case ClientCommandParam:
			err = makeHTTPRequest(clientReportResource, nil, body, http.DefaultClient, nil, nil)
		case EndpointCommandParam:
			err = makeHTTPRequest(endpointReportResource, nil, body, http.DefaultClient, nil, nil)
		}

		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[StatCommand] = func(args []string) (string, error) {
		query := url.Values{}
		keypairs := strings.Split(args[0], ",")
		for _, pair := range keypairs {
			values := strings.Split(pair, "=")
			query.Add(values[0], values[1])
		}

		var result string
		err := makeHTTPRequest(statQueryResource, query, nil, http.DefaultClient, stringDecoder, &result)
		if err != nil {
			return "", err
		}
		return result, nil
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
			"\tROUTE CLIENT <ip> <content_id>\n",
			"\tROUTE ENDPOINT <ip>\n",
			"\tALLOCATE <region_id> <available_bytes>\n",
			"\tSREGION <region_id> <public_addr> <private_addr>\n",
			"\tUREGION <region_id>\n",
			"\tREPORT CLIENT <json_report>\n",
			"\tREPORT ENDPOINT <json_report>\n",
			"\tSTAT <key=value,key=value,...>\n",
			"\tEXIT\n",
		), nil
	}
	return actions
}
