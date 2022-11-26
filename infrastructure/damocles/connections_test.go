package damocles

import "testing"

func TestEndpointConnectionManager(t *testing.T) {
	category := "fid"
	ws := &mockWebsocket{
		msgs:       [][]byte{[]byte{}},
		writeCount: 0,
	}
	connections := NewEndpointConnectionManager()

	// Test create and put
	if err := connections.CreateCategory(category); err != nil {
		t.Fatalf("Failed to create category %s: %v", category, err)
	}
	if err := connections.Put(category, ws); err != nil {
		t.Fatalf("Failed to put websocket in category: %v", err)
	}

	// Test pop and empty pop
	if _, err := connections.Pop(category); err != nil {
		t.Fatalf("Failed to pop websocket from category: %v", err)
	}
	if _, err := connections.Pop(category); err == nil {
		t.Fatalf("Should have failed to pop websocket from category as category is empty")
	}

	// Test del and put on non-existant category
	if err := connections.DelCategory(category); err != nil {
		t.Fatalf("Failed to delete ctaegory %s: %v", category, err)
	}
	if err := connections.Put(category, &mockWebsocket{}); err == nil {
		t.Fatalf("Should have failed to put websocket in category as category doesn't exist")
	}
}
