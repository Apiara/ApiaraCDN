package state

import "errors"

var errMock error = errors.New("mock microservice state failed")

type MockMicroserviceState struct {
	store map[string]interface{}
}

func NewMockMicroserviceState() *MockMicroserviceState {
	return &MockMicroserviceState{make(map[string]interface{})}
}

func (m *MockMicroserviceState) GetRegionAddress(location string) (string, error) {
	if result, ok := m.store[location]; ok {
		return result.(string), nil
	}
	return "", errMock
}

func (m *MockMicroserviceState) SetRegionAddress(location string, address string) error {
	m.store[location] = address
	return nil
}

func (m *MockMicroserviceState) RemoveRegionAddress(location string) error {
	delete(m.store, location)
	return nil
}

const (
	mockFIDKey      = ":fid"
	mockCIDKey      = ":cid"
	mockSizeKey     = ":size"
	mockResourceKey = ":resource"

	mockRulesKey = "rules:list"
)

func (m *MockMicroserviceState) CreateContentEntry(cid string, fid string, size int64, resources []string) error {
	m.store[cid+mockFIDKey] = fid
	m.store[fid+mockCIDKey] = cid
	m.store[cid+mockSizeKey] = size
	m.store[cid+mockResourceKey] = resources
	return nil
}

func (m *MockMicroserviceState) DeleteContentEntry(cid string) error {
	if fid, ok := m.store[cid+mockFIDKey]; ok {
		delete(m.store, cid+mockFIDKey)
		delete(m.store, fid.(string)+mockCIDKey)
		delete(m.store, cid+mockSizeKey)
		delete(m.store, cid+mockResourceKey)
	}
	return nil
}

func (m *MockMicroserviceState) GetContentFunctionalID(cid string) (string, error) {
	if fid, ok := m.store[cid+mockFIDKey]; ok {
		return fid.(string), nil
	}
	return "", errMock
}

func (m *MockMicroserviceState) GetContentID(fid string) (string, error) {
	if fid, ok := m.store[fid+mockCIDKey]; ok {
		return fid.(string), nil
	}
	return "", errMock
}

func (m *MockMicroserviceState) GetContentResources(cid string) ([]string, error) {
	if resources, ok := m.store[cid+mockResourceKey]; ok {
		return resources.([]string), nil
	}
	return nil, errMock
}

func (m *MockMicroserviceState) GetContentSize(cid string) (int64, error) {
	if size, ok := m.store[cid+mockSizeKey]; ok {
		return size.(int64), nil
	}
	return -1, errMock
}

func (m *MockMicroserviceState) CreateContentLocationEntry(cid string, serverID string, pulled bool) error {
	m.store[cid+serverID] = true
	return nil
}

func (m *MockMicroserviceState) DeleteContentLocationEntry(cid string, serverID string) error {
	delete(m.store, cid+serverID)
	return nil
}

func (m *MockMicroserviceState) IsContentServedByServer(cid string, serverID string) (bool, error) {
	_, ok := m.store[cid+serverID]
	return ok, nil
}

func (m *MockMicroserviceState) ContentServerList(cid string) ([]string, error) {
	return nil, nil
}

func (m *MockMicroserviceState) ServerContentList(server string) ([]string, error) {
	return nil, nil
}

func (m *MockMicroserviceState) IsContentBeingServed(cid string) (bool, error) {
	return false, nil
}

func (m *MockMicroserviceState) WasContentPulled(cid string, serverID string) (bool, error) {
	return true, nil
}

func (m *MockMicroserviceState) GetContentPullRules() ([]string, error) {
	if rules, ok := m.store[mockRulesKey]; ok {
		return rules.([]string), nil
	}
	return []string{}, nil
}

func (m *MockMicroserviceState) ContentPullRuleExists(rule string) (bool, error) {
	rules, err := m.GetContentPullRules()
	if err != nil {
		return false, err
	}

	for _, ruleCandidate := range rules {
		if ruleCandidate == rule {
			return true, nil
		}
	}
	return false, nil
}

func (m *MockMicroserviceState) CreateContentPullRule(rule string) error {
	var newRules []string
	if rules, ok := m.store[mockRulesKey]; ok {
		newRules = rules.([]string)
	} else {
		newRules = []string{}
	}
	newRules = append(newRules, rule)
	m.store[mockRulesKey] = newRules
	return nil
}

func (m *MockMicroserviceState) DeleteContentPullRule(rule string) error {
	rules, err := m.GetContentPullRules()
	if err != nil {
		return err
	}

	for i, ruleCandidate := range rules {
		if ruleCandidate == rule {
			newRules := append(rules[:i], rules[i+1:]...)
			m.store[mockRulesKey] = newRules
			return nil
		}
	}
	return nil
}
