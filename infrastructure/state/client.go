package state

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

/*
MicroserviceStateAPIClient implements MicroserviceState by communicating
to the Microservice State Client and having it perform state operations
on behalf of the client
*/
type MicroserviceStateAPIClient struct {
	client *http.Client

	getFunctionalID            string
	getContentID               string
	getContentResources        string
	getContentSize             string
	createContentEntry         string
	deleteContentEntry         string
	createServerEntry          string
	deleteServerEntry          string
	getServerPublicAddr        string
	getServerPrivateAddr       string
	getAllServers              string
	isServerServing            string
	getServerList              string
	getContentList             string
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
		infra.StateAPIGetFunctionalIDResource, infra.StateAPIGetContentIDResource, infra.StateAPIGetContentResourcesResource,
		infra.StateAPIGetContentSizeResource, infra.StateAPICreateContentEntryResource, infra.StateAPIDeleteContentEntryResource,
		infra.StateAPICreateServerEntryResource, infra.StateAPIDeleteServerEntryResource, infra.StateAPIGetServerPublicAddressResource,
		infra.StateAPIGetServerPrivateAddressResource, infra.StateAPIGetServerListResource, infra.StateAPIIsServerServingResource,
		infra.StateAPIGetContentServerListResource, infra.StateAPIGetServerContentListResource, infra.StateAPIIsContentActiveResource,
		infra.StateAPIWasContentPulledResource, infra.StateAPICreateContentLocationEntryResource, infra.StateAPIDeleteContentLocationEntryResource,
		infra.StateAPIGetContentPullRulesResource, infra.StateAPIDoesRuleExistResource, infra.StateAPICreateContentPullRuleResource,
		infra.StateAPIDeleteContentPullRuleResource,
	}

	var err error
	apiEndpoints := make([]string, len(apiResources))
	for i, resource := range apiResources {
		apiEndpoints[i], err = url.JoinPath(strings.TrimSpace(stateServiceAPI), resource)
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
		apiEndpoints[16], apiEndpoints[17], apiEndpoints[18], apiEndpoints[19],
		apiEndpoints[20], apiEndpoints[21],
	}, nil
}

func (c *MicroserviceStateAPIClient) GetContentFunctionalID(cid string) (string, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result string
	if err := infra.MakeHTTPRequest(c.getFunctionalID, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return "", fmt.Errorf("failed to get functional ID for content(%s): %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) GetContentID(fid string) (string, error) {
	query := url.Values{}
	query.Add(FunctionalIDHeader, fid)

	var result string
	if err := infra.MakeHTTPRequest(c.getContentID, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return "", fmt.Errorf("failed to get content id for functional id(%s): %w", fid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) GetContentResources(cid string) ([]string, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result []string
	if err := infra.MakeHTTPRequest(c.getContentResources, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return nil, fmt.Errorf("failed to get resources for content(%s): %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) GetContentSize(cid string) (int64, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result int64
	if err := infra.MakeHTTPRequest(c.getContentSize, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
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
	if err = infra.MakeHTTPRequest(c.createContentEntry, url.Values{}, &body, c.client, nil, nil); err != nil {
		return fmt.Errorf(errMsg, cid, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) DeleteContentEntry(cid string) error {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	if err := infra.MakeHTTPRequest(c.deleteContentEntry, query, nil, c.client, nil, nil); err != nil {
		return fmt.Errorf("failed to delete content(%s) entry: %w", cid, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) CreateServerEntry(sid string, publicAddr string, privateAddr string) error {
	query := url.Values{}
	query.Add(ServerHeader, sid)
	query.Add(ServerPublicAddrHeader, publicAddr)
	query.Add(ServerPrivateAddrHeader, privateAddr)
	if err := infra.MakeHTTPRequest(c.createServerEntry, query, nil, c.client, nil, nil); err != nil {
		return fmt.Errorf("failed to create server(%s) entry: %w", sid, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) DeleteServerEntry(sid string) error {
	query := url.Values{}
	query.Add(ServerHeader, sid)
	if err := infra.MakeHTTPRequest(c.deleteServerEntry, query, nil, c.client, nil, nil); err != nil {
		return fmt.Errorf("failed to delete server(%s) entry: %w", sid, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) GetServerPublicAddress(sid string) (string, error) {
	query := url.Values{}
	query.Add(ServerHeader, sid)

	var result string
	if err := infra.MakeHTTPRequest(c.getServerPublicAddr, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return "", fmt.Errorf("failed to get server(%s) public address: %w", sid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) GetServerPrivateAddress(sid string) (string, error) {
	query := url.Values{}
	query.Add(ServerHeader, sid)

	var result string
	if err := infra.MakeHTTPRequest(c.getServerPrivateAddr, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return "", fmt.Errorf("failed to get server(%s) private address: %w", sid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) ServerList() ([]string, error) {
	var result []string
	if err := infra.MakeHTTPRequest(c.getAllServers, nil, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return nil, fmt.Errorf("failed to get all servers: %w", err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) IsContentServedByServer(cid string, server string) (bool, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)
	query.Add(ServerHeader, server)

	var result bool
	if err := infra.MakeHTTPRequest(c.isServerServing, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return false, fmt.Errorf("failed to check if content(%s) served by server(%s): %w", cid, server, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) ContentServerList(cid string) ([]string, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result []string
	if err := infra.MakeHTTPRequest(c.getServerList, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return nil, fmt.Errorf("failed to get server list for content(%s): %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) ServerContentList(server string) ([]string, error) {
	query := url.Values{}
	query.Add(ServerHeader, server)

	var result []string
	if err := infra.MakeHTTPRequest(c.getContentList, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return nil, fmt.Errorf("failed to get content list for server(%s): %w", server, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) IsContentBeingServed(cid string) (bool, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)

	var result bool
	if err := infra.MakeHTTPRequest(c.isContentActive, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return false, fmt.Errorf("failed to check if content(%s) is active: %w", cid, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) WasContentPulled(cid string, server string) (bool, error) {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)
	query.Add(ServerHeader, server)

	var result bool
	if err := infra.MakeHTTPRequest(c.wasContentPulled, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return false, fmt.Errorf("failed to check if content(%s) was pulled to server(%s): %w", cid, server, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) CreateContentLocationEntry(cid string, server string, pulled bool) error {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)
	query.Add(ServerHeader, server)
	query.Add(ContentWasPulledHeader, strconv.FormatBool(pulled))

	if err := infra.MakeHTTPRequest(c.createContentLocationEntry, query, nil, c.client, nil, nil); err != nil {
		return fmt.Errorf("failed to create content(%s) to server(%s) location entry: %w", cid, server, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) DeleteContentLocationEntry(cid string, server string) error {
	query := url.Values{}
	query.Add(ContentIDHeader, cid)
	query.Add(ServerHeader, server)

	if err := infra.MakeHTTPRequest(c.deleteContentLocationEntry, query, nil, c.client, nil, nil); err != nil {
		return fmt.Errorf("failed to delete content(%s) to server(%s) location entry: %w", cid, server, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) GetContentPullRules() ([]string, error) {
	var result []string
	if err := infra.MakeHTTPRequest(c.getPullRules, nil, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return nil, fmt.Errorf("failed to get content pull rules: %w", err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) ContentPullRuleExists(rule string) (bool, error) {
	query := url.Values{}
	query.Add(RuleHeader, rule)

	var result bool
	if err := infra.MakeHTTPRequest(c.pullRuleExist, query, nil, c.client, infra.GOBBodyDecoder, &result); err != nil {
		return false, fmt.Errorf("failed to check if pull rule(%s) exists: %w", rule, err)
	}
	return result, nil
}

func (c *MicroserviceStateAPIClient) CreateContentPullRule(rule string) error {
	query := url.Values{}
	query.Add(RuleHeader, rule)

	if err := infra.MakeHTTPRequest(c.createPullRule, query, nil, c.client, nil, nil); err != nil {
		return fmt.Errorf("failed to create pull rule(%s): %w", rule, err)
	}
	return nil
}

func (c *MicroserviceStateAPIClient) DeleteContentPullRule(rule string) error {
	query := url.Values{}
	query.Add(RuleHeader, rule)

	if err := infra.MakeHTTPRequest(c.deletePullRule, query, nil, c.client, nil, nil); err != nil {
		return fmt.Errorf("failed to delete pull rule(%s): %w", rule, err)
	}
	return nil
}
