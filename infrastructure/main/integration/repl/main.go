package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

const (
	PushCommand      = "push"
	PurgeCommand     = "purge"
	SetRuleCommand   = "srule"
	UnsetRuleCommand = "urule"
	RouteCommand     = "locate"
	AllocateCommand  = "allocate"

	ClientRouteParam   = "client"
	EndpointRouteParam = "endpoint"

	SuccessResult = "Success"
)

type replConfig struct {
	RoutePort          int `toml:"route_port"`
	AllocatorPort      int `toml:"allocator_port"`
	ContentManagerPort int `toml:"content_manager_port"`
	RuleManagerPort    int `toml:"rule_manager_port"`
}

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
	localhost := "http://127.0.0.1/"
	pushResource := localhost + strconv.Itoa(conf.ContentManagerPort) + infra.DeusServiceAPIPushResource
	purgeResource := localhost + strconv.Itoa(conf.ContentManagerPort) + infra.DeusServiceAPIPurgeResource
	setRuleResource := localhost + strconv.Itoa(conf.RuleManagerPort) + infra.ReikoServiceAPIAddRuleResource
	unsetRuleResource := localhost + strconv.Itoa(conf.RuleManagerPort) + infra.ReikoServiceAPIDelRuleResource
	clientLocateResource := localhost + strconv.Itoa(conf.RoutePort) + infra.AmadaRouteAPIClientResource
	endpointLocateResource := localhost + strconv.Itoa(conf.RoutePort) + infra.AmadaRouteAPIEndpointResource
	allocateResource := localhost + strconv.Itoa(conf.AllocatorPort) + infra.CrowAllocateAPIResource

	actions := make(map[string]action)
	actions[PushCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentIDHeader, args[0])
		query.Add(infra.ServerIDHeader, args[1])

		err := makeHTTPRequest(pushResource, query, nil, http.DefaultClient, nil)
		if err != nil {
			return "", err
		}
		return SuccessResult, nil
	}
	actions[PurgeCommand] = func(args []string) (string, error) {
		query := url.Values{}
		query.Add(infra.ContentIDHeader, args[0])
		query.Add(infra.ServerIDHeader, args[1])

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
		query.Add(infra.LocationHeader, args[0])
		query.Add(infra.ByteSizeHeader, args[1])

		err := makeHTTPRequest(allocateResource, query, nil, http.DefaultClient, &result)
		if err != nil {
			return "", err
		}
		return result, nil
	}
	return actions
}

func startREPL(actionMap map[string]action) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("ApiaraCDN Integration Testing REPL")
	fmt.Println("----------------------------------")

	for {
		fmt.Printf("> ")
		input, _ := reader.ReadString('\n')
		args := strings.Split(strings.Trim(input, " \t\n"), " ")

		if action, ok := actionMap[args[0]]; ok {
			response, err := action(args[1:])
			if err != nil {
				fmt.Printf("Error: %s\n", err.Error())
			} else {
				fmt.Println(response)
			}
		} else {
			fmt.Println("Error: invalid command")
		}
	}
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file")
	flag.Parse()

	var conf replConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}

	actionMap := createActionMap(conf)
	startREPL(actionMap)
}
