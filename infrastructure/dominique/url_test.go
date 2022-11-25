package dominique

import (
  "testing"
  "github.com/stretchr/testify/assert"
)

func TestRedisURLIndex(t *testing.T) {
  addr := ":7777"
  fid := "test"
  correctURL := "test_url"

  finder := NewRedisURLIndex(addr)
  url, err := finder.FunctionalIDToURL(fid)
  if err != nil {
    t.Fatalf("Failed to retrieved URL: %v", err)
  }
  assert.Equal(t, url, correctURL, "Retrieved wrong url: " + url)
}
