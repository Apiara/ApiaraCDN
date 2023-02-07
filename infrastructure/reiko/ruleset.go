package reiko

import (
	"fmt"
	"strings"
)

type ContentRuleStore interface {
	GetContentPullRules() ([]string, error)
	ContentPullRuleExists(rule string) (bool, error)
	CreateContentPullRule(rule string) error
	DeleteContentPullRule(rule string) error
}

// ContentRules allows modification of valid content rules as well as rule checking
type ContentRules interface {
	DoesContentMatchRule(cid string) (bool, error)
	CreateContentPullRule(rule string) error
	DeleteContentPullRule(rule string) error
}

type PrefixContentRules struct {
	store ContentRuleStore
}

func NewPrefixContentRules(store ContentRuleStore) *PrefixContentRules {
	return &PrefixContentRules{store}
}

func (p *PrefixContentRules) DoesContentMatchRule(cid string) (bool, error) {
	rules, err := p.store.GetContentPullRules()
	if err != nil {
		return false, fmt.Errorf("failed to check if content(%s) matches a rule: %w", cid, err)
	}

	for _, rule := range rules {
		if strings.HasPrefix(cid, rule) {
			return true, nil
		}
	}
	return false, nil
}

func (p *PrefixContentRules) CreateContentPullRule(rule string) error {
	return p.store.CreateContentPullRule(rule)
}

func (p *PrefixContentRules) DeleteContentPullRule(rule string) error {
	return p.store.DeleteContentPullRule(rule)
}
