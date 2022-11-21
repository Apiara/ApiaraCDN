package deus

import (
  "net"
  "fmt"
  "github.com/oschwald/geoip2-golang"
)

// IPGeoFinder provides IP Address to Region lookups
type IPGeoFinder interface {
  Location(ip string) (string, error)
}

// A region is a rectangular region of space defined by two latitude, longitude pairs
type Region struct {
  Name string
  MinLatitude float64
  MaxLatitude float64
  MinLongitude float64
  MaxLongitude float64
}

// Contains returns whether or not the latitude, longitude pair resides in the region
func (r *Region) Contains(lat float64, long float64) bool {
  return lat >= r.MinLatitude && lat <= r.MaxLatitude &&
    long >= r.MinLongitude && long <= r.MaxLongitude
}

/* MaxMindIPGeoFinder uses MaxMind database files to find IP->coordinate mappings
which are then used to figure out what region an IP address may be in */
type MaxMindIPGeoFinder struct {
  db *geoip2.Reader
  regions []Region
}

// NewMaxMindIPGeoFinder returns a MaxMindIPGeoFinder that uses the provided .mmdb file
func NewMaxMindIPGeoFinder(mmdbFile string, regions []Region) (*MaxMindIPGeoFinder, error) {
  db, err := geoip2.Open(mmdbFile)
  if err != nil {
    return nil, err
  }
  return &MaxMindIPGeoFinder{
    db: db,
    regions: regions,
  }, nil
}

// Location returns the region name for the provided IP
func (m *MaxMindIPGeoFinder) Location(ipStr string) (string, error) {
  ip := net.ParseIP(ipStr)
  record, err := m.db.City(ip)
  if err != nil {
    return "", err
  }

  latitude := record.Location.Latitude
  longitude := record.Location.Longitude
  for _, possibleRegion := range m.regions {
    if possibleRegion.Contains(latitude, longitude) {
      return possibleRegion.Name, nil
    }
  }
  return "", fmt.Errorf("Failed to find region for %s", ipStr)
}
