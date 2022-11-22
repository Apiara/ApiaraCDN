package deus

import (
  "testing"
)

func TestMaxMindIPGeoFinder(t *testing.T) {
  // Create test resources
  mmdbFile := "test_resources/GeoLite2-City.mmdb"
  ip := "144.86.149.32"
  actualLoc := "Oregon"
  regions := []Region{
    Region{
      Name: "Oregon",
      MinLatitude: 42.042163,
      MaxLatitude: 45.814558,
      MinLongitude: -124.204614,
      MaxLongitude: -116.807508,
    },
  }

  geoFinder, err := NewMaxMindIPGeoFinder(mmdbFile, regions)
  if err != nil {
    t.Fatalf("Failed to create new MaxMind geo finder: %v", err)
  }

  // Check if Oregon IP is mapped to Oregon region
  foundLoc, err := geoFinder.Location(ip)
  if err != nil {
    t.Fatalf("Failed to run location finder: %v", err)
  }
  if foundLoc != actualLoc {
    t.Fatalf("Returned wrong location(%s) -> actual location(%s)", foundLoc, actualLoc)
  }


  // Check if England IP fails to map to any region
  englandIP := "31.10.47.255"
  foundLoc, err = geoFinder.Location(englandIP)
  if err == nil {
    t.Fatalf("Found location when should have failed -> location found: %s", foundLoc)
  }
}
