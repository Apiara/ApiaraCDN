package reiko

import (
	"testing"

	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

func TestPrefixContentRules(t *testing.T) {
	ruleStore := state.NewMockMicroserviceState()
	ruleset := NewPrefixContentRules(ruleStore)

	// Test empty match
	cid := "testcid"
	match, err := ruleset.DoesContentMatchRule(cid)
	if err != nil {
		t.Fatalf("Failed to run MatchesRule on non-existant cid: %v\n", err)
	}
	if match {
		t.Fatal("Found rule match when should have found no match")
	}

	// Test rule set and valid match
	prefix := "test"
	if err = ruleset.CreateContentPullRule(prefix); err != nil {
		t.Fatalf("Failed to set rule: %v\n", err)
	}
	match, err = ruleset.DoesContentMatchRule(cid)
	if err != nil {
		t.Fatalf("Failed to run MatchesRule after rule set: %v\n", err)
	}
	if !match {
		t.Fatal("Failed to rule match when should have matched")
	}

	// Test rule deletion an invalid match
	if err = ruleset.DeleteContentPullRule(prefix); err != nil {
		t.Fatalf("Failed to delete rule: %v\n", err)
	}

	badPrefix := "temp"
	if err = ruleset.CreateContentPullRule(badPrefix); err != nil {
		t.Fatalf("Failed to set rule: %v\n", err)
	}
	match, err = ruleset.DoesContentMatchRule(cid)
	if err != nil {
		t.Fatalf("Failed to run MatchesRule after rule set: %v\n", err)
	}
	if match {
		t.Fatal("Found rule match when should not have matched")
	}

	// Cleanup
	if ruleset.DeleteContentPullRule(badPrefix); err != nil {
		t.Fatalf("Failed to delete rule: %v\n", err)
	}
}
