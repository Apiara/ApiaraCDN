package main

import (
	"flag"
	"strconv"

	"github.com/Apiara/ApiaraCDN/infrastructure/amada"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
Configuration Format
--------------------
override_listen_port = int
route_listen_port = int
mmdb_geo_file = string

pull_decider_api = string

redis_address = string

[regions.pnw]
  min_latitude = float64
  max_latitude = float64
  min_longitude = float64
  max_longitude = float64
*/

type (
	deusConfig struct {
		OverrideListenPort    int    `toml:"override_listen_port"`
		RouteListenPort       int    `toml:"route_listen_port"`
		MaxMindGeoFile        string `toml:"mmdb_geo_file"`
		PullDeciderAPIAddress string `toml:"pull_decider_api"`
		RedisAddress          string `toml:"redis_address"`
		Regions               map[string]region
	}

	region struct {
		MinLatitude  float64 `toml:"min_latitude"`
		MaxLatitude  float64 `toml:"max_latitude"`
		MinLongitude float64 `toml:"min_longitude"`
		MaxLongitude float64 `toml:"max_longitude"`
	}
)

func main() {
	// Parse configuration
	fnamePtr := flag.String("config", "", "TOML configuration file")
	flag.Parse()

	var conf deusConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}

	// Process configuration parameters
	regions := []amada.Region{}
	for name, region := range conf.Regions {
		regions = append(regions, amada.Region{
			Name:         name,
			MinLatitude:  region.MinLatitude,
			MaxLatitude:  region.MaxLatitude,
			MinLongitude: region.MinLongitude,
			MaxLongitude: region.MaxLongitude,
		})
	}

	overrideListenAddr := ":" + strconv.Itoa(conf.OverrideListenPort)
	routeListenAddr := ":" + strconv.Itoa(conf.RouteListenPort)

	// Create resources
	microserviceState, err := state.NewMicroserviceStateAPIClient(conf.RedisAddress)
	if err != nil {
		panic(err)
	}

	geoFinder, err := amada.NewMaxMindIPGeoFinder(conf.MaxMindGeoFile, regions)
	if err != nil {
		panic(err)
	}

	// Start APIs
	go amada.StartServiceAPI(overrideListenAddr, microserviceState, geoFinder)
	amada.StartDeviceRoutingAPI(routeListenAddr, geoFinder, microserviceState,
		microserviceState, conf.PullDeciderAPIAddress)
}
