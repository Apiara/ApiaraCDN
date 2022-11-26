package reiko

import (
	"testing"
	"time"
)

func TestPrefixContentRules(t *testing.T) {
	RuleSetRefreshDuration = time.Duration(0)
	redisTestAddress := ":7777"
	ruleset := NewPrefixContentRules(redisTestAddress)

	// Test empty match
	cid := "testcid"
	match, err := ruleset.MatchesRule(cid)
	if err != nil {
		t.Fatalf("Failed to run MatchesRule on non-existant cid: %v\n", err)
	}
	if match {
		t.Fatal("Found rule match when should have found no match")
	}

	// Test rule set and valid match
	prefix := "test"
	if err = ruleset.SetRule(prefix); err != nil {
		t.Fatalf("Failed to set rule: %v\n", err)
	}
	match, err = ruleset.MatchesRule(cid)
	if err != nil {
		t.Fatalf("Failed to run MatchesRule after rule set: %v\n", err)
	}
	if !match {
		t.Fatal("Failed to rule match when should have matched")
	}

	// Test rule deletion an invalid match
	if err = ruleset.DelRule(prefix); err != nil {
		t.Fatalf("Failed to delete rule: %v\n", err)
	}

	badPrefix := "temp"
	if err = ruleset.SetRule(badPrefix); err != nil {
		t.Fatalf("Failed to set rule: %v\n", err)
	}
	match, err = ruleset.MatchesRule(cid)
	if err != nil {
		t.Fatalf("Failed to run MatchesRule after rule set: %v\n", err)
	}
	if match {
		t.Fatal("Found rule match when should not have matched")
	}

	// Cleanup
	if ruleset.DelRule(badPrefix); err != nil {
		t.Fatalf("Failed to delete rule: %v\n", err)
	}
}
