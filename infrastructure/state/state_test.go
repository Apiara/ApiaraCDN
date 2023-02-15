package state

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisMicroserviceState(t *testing.T) {
	// Setup primary service
	redisAddr := ":7777"
	primaryState := NewRedisMicroserviceState(redisAddr)
	port := ":12346"
	go StartDataService(port, primaryState)
	time.Sleep(time.Second)

	// Create relay client
	microserviceState, err := NewMicroserviceStateAPIClient("http://127.0.0.1" + port)
	if err != nil {
		t.Fatal(err)
	}

	// Test region mappings
	region := "Oregon"
	server := "server"
	if err := microserviceState.SetRegionAddress(region, server); err != nil {
		t.Fatalf("Failed to set region address: %v\n", err)
	}

	sids, err := microserviceState.ServerList()
	assert.Nil(t, err, "error should be nil for ServerList")
	assert.Len(t, sids, 1, "server list should be 1")
	assert.Equal(t, server, sids[0], "server values should match")

	retAddress, err := microserviceState.GetRegionAddress(region)
	if err != nil {
		t.Fatalf("Failed to get region address: %v\n", err)
	}
	if retAddress != server {
		t.Fatalf("Failed to get correct region address. Got %s instead\n", retAddress)
	}

	if err = microserviceState.RemoveRegionAddress(region); err != nil {
		t.Fatalf("Failed to remove region address: %v\n", err)
	}
	retAddress, err = microserviceState.GetRegionAddress(region)
	if err == nil {
		t.Fatalf("Found region address when should have failed: %s\n", retAddress)
	}

	// Test content information + propogation to location
	cid := "http://www.random.com/something"
	fid := "functionalID"
	size := int64(1024)
	resources := []string{"random", "random2", "random3"}
	resourceMap := make(map[string]bool)
	for _, resource := range resources {
		resourceMap[resource] = true
	}

	if err := microserviceState.CreateContentEntry(cid, fid, size, resources); err != nil {
		t.Fatalf("Failed to create entry: %v", err)
	}
	defer microserviceState.DeleteContentEntry(cid)

	foundResources, err := microserviceState.GetContentResources(cid)
	if err != nil {
		t.Fatalf("Failed to get resources: %v", err)
	}
	for _, found := range foundResources {
		if _, ok := resourceMap[found]; !ok {
			t.Fatalf("Failed to return valid resource. Got %s", found)
		}
	}

	foundFid, err := microserviceState.GetContentFunctionalID(cid)
	if err != nil {
		t.Fatalf("Failed to get functional id: %v", err)
	}
	assert.Equal(t, foundFid, fid, "Functional IDs not equal")

	foundCid, err := microserviceState.GetContentID(fid)
	if err != nil {
		t.Fatalf("Failed to get content id: %v", err)
	}
	assert.Equal(t, foundCid, cid, "Content IDs not equal")

	foundSize, err := microserviceState.GetContentSize(cid)
	if err != nil {
		t.Fatalf("Failed to get content size: %v", err)
	}
	assert.Equal(t, foundSize, size, "Sizes are not equal")

	if err = microserviceState.DeleteContentEntry(cid); err != nil {
		t.Fatalf("Failed to delete entry: %v", err)
	}

	// Test content<->server mapping lists after Content Location Entry created
	servers, err := microserviceState.ContentServerList(cid)
	assert.Nil(t, err, "ContentServerList error return should be nil")
	assert.Equal(t, 0, len(servers), "Server list should be size 0")

	cids, err := microserviceState.ServerContentList(server)
	assert.Nil(t, err, "ServerContentList error return should be nil")
	assert.Equal(t, 0, len(cids), "Content list should be size 0")

	// Test content location
	if err := microserviceState.CreateContentLocationEntry(cid, server, true); err != nil {
		t.Fatalf("Failed to set content serving state: %v\n", err)
	}

	serving, err := microserviceState.IsContentServedByServer(cid, server)
	if err != nil {
		t.Fatalf("Failed to check if content being served: %v\n", err)
	}
	if !serving {
		t.Fatalf("Failed to see that content is being served\n")
	}

	dyn, err := microserviceState.WasContentPulled(cid, server)
	if err != nil {
		t.Fatalf("Failed to check if content was dynamically set: %v\n", err)
	}
	if !dyn {
		t.Fatalf("Failed to see that content was set dynamically\n")
	}

	// Test content<->server mapping lists after Content Location Entry created
	servers, err = microserviceState.ContentServerList(cid)
	assert.Nil(t, err, "ContentServerList error return should be nil")
	assert.Equal(t, 1, len(servers), "Server list should be size 1")
	assert.Equal(t, server, servers[0], "Server returned in Server List was wrong")

	cids, err = microserviceState.ServerContentList(server)
	assert.Nil(t, err, "ServerContentList error return should be nil")
	assert.Equal(t, 1, len(cids), "Content list should be size 1")
	assert.Equal(t, cid, cids[0], "Content returned in Content List was wrong")

	if err = microserviceState.DeleteContentLocationEntry(cid, server); err != nil {
		t.Fatalf("Failed to remove content serve state: %v\n", err)
	}
	serving, err = microserviceState.IsContentServedByServer(cid, server)
	if err != nil {
		t.Fatalf("Failed to check if content being served: %v\n", err)
	}
	if serving {
		t.Fatalf("Failed to see that content is not being served\n")
	}

	// Test pull rules
	match, err := microserviceState.ContentPullRuleExists(cid)
	if err != nil {
		t.Fatalf("Failed to run MatchesRule on non-existant cid: %v\n", err)
	}
	if match {
		t.Fatal("Found rule match when should have found no match")
	}

	if err = microserviceState.CreateContentPullRule(cid); err != nil {
		t.Fatalf("Failed to set rule: %v\n", err)
	}
	match, err = microserviceState.ContentPullRuleExists(cid)
	if err != nil {
		t.Fatalf("Failed to run MatchesRule after rule set: %v\n", err)
	}
	if !match {
		t.Fatal("Failed to rule match when should have matched")
	}

	if err = microserviceState.DeleteContentPullRule(cid); err != nil {
		t.Fatalf("Failed to delete rule: %v\n", err)
	}

}
