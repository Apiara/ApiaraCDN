package main

import (
  "flag"
  "time"
  "strconv"
  "io/ioutil"
  "github.com/BurntSushi/toml"
  "github.com/Apiara/ApiaraCDN/infrastructure/deus"
)
/*
Configuration Format
--------------------

pull_frequency = time.Duration
pull_request_threshold = int
override_listen_port = int
route_listen_port = int
mmdb_geo_file = string

validate_api = string
process_api = string
coordinate_api = string

redis_address = string

[regions.pnw]
  min_latitude = float64
  max_latitude = float64
  min_longitude = float64
  max_longitude = float64
*/

type (
  config struct {
    PullFrequency time.Duration `toml:"pull_frequency"`
    PullRequestThreshold int `toml:"pull_request_threshold"`
    OverrideListenPort int `toml:"override_listen_port"`
    RouteListenPort int `toml:"route_listen_port"`
    MaxMindGeoFile string `toml:"mmdb_geo_file"`
    ValidateAPIAddress string `toml:"validate_api"`
    ProcessAPIAddress string `toml:"process_api"`
    CoordinateAPIAddress string `toml:"coordinate_api"`
    RedisAddress string `toml:"redis_address"`
    Regions map[string]region
  }

  region struct {
    MinLatitude float64 `toml:"min_latitude"`
    MaxLatitude float64 `toml:"max_latitude"`
    MinLongitude float64 `toml:"min_longitude"`
    MaxLongitude float64 `toml:"max_longitude"`
  }
)

func readConfig(fname string) (*config, error) {
  data, err := ioutil.ReadFile(fname)
  if err != nil {
    return nil, err
  }

  var conf config
  _, err = toml.Decode(string(data), &conf)
  if err != nil {
    return nil, err
  }
  return &conf, nil
}

func main() {
  // Parse configuration
  fnamePtr := flag.String("config", "", "TOML configuration file")
  flag.Parse()

  conf, err := readConfig(*fnamePtr)
  if err != nil {
    panic(err)
  }

  // Process configuration parameters
  regions := []deus.Region{}
  for name, region := range conf.Regions {
    regions = append(regions, deus.Region{
      Name: name,
      MinLatitude: region.MinLatitude,
      MaxLatitude: region.MaxLatitude,
      MinLongitude: region.MinLongitude,
      MaxLongitude: region.MaxLongitude,
    })
  }

  overrideListenAddr := ":" + strconv.Itoa(conf.OverrideListenPort)
  routeListenAddr := ":" + strconv.Itoa(conf.RouteListenPort)

  // Create resources
  contentState := deus.NewRedisContentState(conf.RedisAddress)
  serverIndex := deus.NewRedisGeoServerIndex(conf.RedisAddress)
  validator := deus.NewContentValidatorClient(conf.ValidateAPIAddress)
  manager := deus.NewMasterContentManager(contentState, conf.ProcessAPIAddress,
    conf.CoordinateAPIAddress)
  geoFinder, err := deus.NewMaxMindIPGeoFinder(conf.MaxMindGeoFile, regions)
  if err != nil {
    panic(err)
  }
  pullDecider := deus.NewThresholdPullDecider(validator, manager, contentState,
    conf.PullRequestThreshold, conf.PullFrequency)

  // Start APIs
  go deus.StartOverrideAPI(overrideListenAddr, manager, serverIndex, geoFinder)
  deus.StartDeviceRoutingAPI(routeListenAddr, geoFinder, contentState,
    serverIndex, pullDecider)
}
