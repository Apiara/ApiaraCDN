package state

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

/*
MicroserviceStateAPIClient implements MicroserviceState by communicating
to the Microservice State Client and having it perform state operations
on behalf of the client
*/
type MicroserviceStateAPIClient struct {
	client *http.Client

	getRegion                  string
	setRegion                  string
	deleteRegion               string
	getFunctionalID            string
	getContentID               string
	getContentResources        string
	getContentSize             string
	createContentEntry         string
	deleteContentEntry         string
	isServerServing            string
	getServerList              string
	isContentActive            string
	wasContentPulled           string
	createContentLocationEntry string
	deleteContentLocationEntry string
	getPullRules               string
	pullRuleExist              string
	createPullRule             string
	deletePullRule             string
}

/*
NewMicroserviceStateAPIClient creates a new instance of MicroserviceStateAPIClient
referencing the Microservice State Service hosted at address stateServiceAPI
*/
func NewMicroserviceStateAPIClient(stateServiceAPI string) (*MicroserviceStateAPIClient, error) {
	// Ensure data types sent over the wire are registered for gob encoding
	gob.Register(metadataCreate{})

	// Create resource paths
	apiResources := []string{
		infra.StateAPIGetRegionResource, infra.StateAPISetRegionResource, infra.StateAPIDeleteRegionResource,
		infra.StateAPIGetFunctionalIDResource, infra.StateAPIGetContentIDResource, infra.StateAPIGetContentResourcesResource,
		infra.StateAPIGetContentSizeResource, infra.StateAPICreateContentEntryResource, infra.StateAPIDeleteContentEntryResource,
		infra.StateAPIIsServerServingResource, infra.StateAPIGetContentServerListResource, infra.StateAPIIsContentActiveResource,
		infra.StateAPIWasContentPulledResource, infra.StateAPICreateContentLocationEntryResource, infra.StateAPIDeleteContentLocationEntryResource,
		infra.StateAPIGetContentPullRulesResource, infra.StateAPIDoesRuleExistResource, infra.StateAPICreateContentPullRuleResource,
		infra.StateAPIDeleteContentPullRuleResource,
	}

	var err error
	apiEndpoints := make([]string, len(apiResources))
	for i, resource := range apiResources {
		apiEndpoints[i], err = url.JoinPath(stateServiceAPI, resource)
		if err != nil {
			return nil, fmt.Errorf("failed to create microservice API client with address(%s): %w", stateServiceAPI, err)
		}
	}

	// Create and return client
	return &MicroserviceStateAPIClient{
		http.DefaultClient,
		apiEndpoints[0], apiEndpoints[1], apiEndpoints[2], apiEndpoints[3],
		apiEndpoints[4], apiEndpoints[5], apiEndpoints[6], apiEndpoints[7],
		apiEndpoints[8], apiEndpoints[9], apiEndpoints[10], apiEndpoints[11],
		apiEndpoints[12], apiEndpoints[13], apiEndpoints[14], apiEndpoints[15],
		apiEndpoints[16], apiEndpoints[17], apiEndpoints[18],
	}, nil
}

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

func (c *MicroserviceStateAPIClient) GetRegionAddress(location string) (string, error) {
	query := url.Values{}
	query.Add(RegionHeader, location)

	var result string
	if err := makeHTTPRequest(c.getRegion, query, nil, c.client, &result); err != nil {
		return "", fmt.Errorf("failed to get region(%s) address: %w", location, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) SetRegionAddress(location string, address string) error {
	query := url.Values{}
	query.Add(RegionHeader, location)
	query.Add(ServerHeader, address)

	if err := makeHTTPRequest(c.setRegion, query, nil, c.client, nil); err != nil {
		return fmt.Errorf("failed to set region(%s) to server(%s): %w", location, address, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) RemoveRegionAddress(location string) error {
	query := url.Values{}
	query.Add(RegionHeader, location)

	if err := makeHTTPRequest(c.deleteRegion, query, nil, c.client, nil); err != nil {
		return fmt.Errorf("failed to delete region(%s): %w", location, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) GetContentFunctionalID(cid string) (string, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result string
	if err := makeHTTPRequest(c.getFunctionalID, query, nil, c.client, &result); err != nil {
		return "", fmt.Errorf("failed to get functional ID for content(%s): %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) GetContentID(fid string) (string, error) {
	query := url.Values{}
	query.Add(FunctionalIDHeader, fid)

	var result string
	if err := makeHTTPRequest(c.getContentID, query, nil, c.client, &result); err != nil {
		return "", fmt.Errorf("failed to get content id for functional id(%s): %w", fid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) GetContentResources(cid string) ([]string, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result []string
	if err := makeHTTPRequest(c.getContentResources, query, nil, c.client, &result); err != nil {
		return nil, fmt.Errorf("failed to get resources for content(%s): %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) GetContentSize(cid string) (int64, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result int64
	if err := makeHTTPRequest(c.getContentSize, query, nil, c.client, &result); err != nil {
		return -1, fmt.Errorf("failed to get size for content(%s): %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) CreateContentEntry(cid string, fid string, size int64, resources []string) error {
	// Create request body
	errMsg := "failed to create content(%s) entry: %w"
	var body bytes.Buffer
	err := gob.NewEncoder(&body).Encode(metadataCreate{
		ContentID:    cid,
		FunctionalID: fid,
		Size:         size,
		Resources:    resources,
	})

	// send request
	if err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	if err = makeHTTPRequest(c.createContentEntry, url.Values{}, &body, c.client, nil); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) DeleteContentEntry(cid string) error {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	if err := makeHTTPRequest(c.deleteContentEntry, query, nil, c.client, nil); err != nil {
		return fmt.Errorf("failed to delete content(%s) entry: %w", cid, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) IsContentServedByServer(cid string, server string) (bool, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)
	query.Add(ServerHeader, server)

	var result bool
	if err := makeHTTPRequest(c.isServerServing, query, nil, c.client, &result); err != nil {
		return false, fmt.Errorf("failed to check if content(%s) served by server(%s): %w", cid, server, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) ContentServerList(cid string) ([]string, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result []string
	if err := makeHTTPRequest(c.getServerList, query, nil, c.client, &result); err != nil {
		return nil, fmt.Errorf("failed to get server list for content(%s): %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) IsContentBeingServed(cid string) (bool, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result bool
	if err := makeHTTPRequest(c.isContentActive, query, nil, c.client, &result); err != nil {
		return false, fmt.Errorf("failed to check if content(%s) is active: %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) WasContentPulled(cid string, server string) (bool, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)
	query.Add(ServerHeader, server)

	var result bool
	if err := makeHTTPRequest(c.wasContentPulled, query, nil, c.client, &result); err != nil {
		return false, fmt.Errorf("failed to check if content(%s) was pulled to server(%s): %w", cid, server, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) CreateContentLocationEntry(cid string, server string, pulled bool) error {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)
	query.Add(ServerHeader, server)
	query.Add(ContentWasPulledHeader, strconv.FormatBool(pulled))

	if err := makeHTTPRequest(c.createContentLocationEntry, query, nil, c.client, nil); err != nil {
		return fmt.Errorf("failed to create content(%s) to server(%s) location entry: %w", cid, server, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) DeleteContentLocationEntry(cid string, server string) error {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)
	query.Add(ServerHeader, server)

	if err := makeHTTPRequest(c.deleteContentLocationEntry, query, nil, c.client, nil); err != nil {
		return fmt.Errorf("failed to delete content(%s) to server(%s) location entry: %w", cid, server, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) GetContentPullRules() ([]string, error) {
	var result []string
	if err := makeHTTPRequest(c.getPullRules, nil, nil, c.client, &result); err != nil {
		return nil, fmt.Errorf("failed to get content pull rules: %w", err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) ContentPullRuleExists(rule string) (bool, error) {
	query := url.Values{}
	query.Add(RuleHeader, rule)

	var result bool
	if err := makeHTTPRequest(c.pullRuleExist, query, nil, c.client, &result); err != nil {
		return false, fmt.Errorf("failed to check if pull rule(%s) exists: %w", rule, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) CreateContentPullRule(rule string) error {
	query := url.Values{}
	query.Add(RuleHeader, rule)

	if err := makeHTTPRequest(c.createPullRule, query, nil, c.client, nil); err != nil {
		return fmt.Errorf("failed to create pull rule(%s): %w", rule, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) DeleteContentPullRule(rule string) error {
	query := url.Values{}
	query.Add(RuleHeader, rule)

	if err := makeHTTPRequest(c.deletePullRule, query, nil, c.client, nil); err != nil {
		return fmt.Errorf("failed to delete pull rule(%s): %w", rule, err)
	}
	return nil
}
