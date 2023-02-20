package deus

import (
	"net/http"
	"net/url"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
)

// ContentValidator represents an object that can check if a cid is valid
type ContentValidator interface {
	IsValid(cid string) (bool, error)
}

// mockContentValidator is a mock implementation for testing
type mockContentValidator struct{}

func (m *mockContentValidator) IsValid(string) (bool, error) { return true, nil }

/*
ContentValidatorClient implements ContentValidator by delegating the validity
check to a standalone content rule manager service
*/
type ContentValidatorClient struct {
	validatorAddr string
	httpClient    *http.Client
}

// NewContentValidatorClient returns a new ContentValidatorClient
func NewContentValidatorClient(validatorAddr string) (*ContentValidatorClient, error) {
	validatorResource, err := url.JoinPath(validatorAddr, infra.ReikoServiceAPIValidateResource)
	if err != nil {
		return nil, err
	}

	return &ContentValidatorClient{
		validatorAddr: validatorResource,
		httpClient:    http.DefaultClient,
	}, nil
}

// IsValid checks with the content rule manager service if cid is valid
func (v *ContentValidatorClient) IsValid(cid string) (bool, error) {
	validateReq, err := http.NewRequest("GET", v.validatorAddr, nil)
	if err != nil {
		return false, err
	}

	query := url.Values{}
	query.Add(infra.ContentIDParam, cid)
	validateReq.URL.RawQuery = query.Encode()

	resp, err := v.httpClient.Do(validateReq)
	if err != nil {
		return false, err
	}
	if resp.StatusCode != http.StatusOK {
		return false, nil
	}
	return true, nil
}
