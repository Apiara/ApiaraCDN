package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	"github.com/Apiara/ApiaraCDN/infrastructure/amada"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
Configuration Format
--------------------
debugging_mode = bool
override_listen_port = int
route_listen_port = int
mmdb_geo_file = string

pull_decider_api = string

state_address = string

[regions.pnw]
  min_latitude = float64
  max_latitude = float64
  min_longitude = float64
  max_longitude = float64
*/

type (
	amadaConfig struct {
		DebuggingMode         bool   `toml:"debugging_mode"`
		OverrideListenPort    int    `toml:"override_listen_port"`
		RouteListenPort       int    `toml:"route_listen_port"`
		MaxMindGeoFile        string `toml:"mmdb_geo_file"`
		PullDeciderAPIAddress string `toml:"pull_decider_api"`
		StateServiceAddress   string `toml:"state_address"`
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

	var conf amadaConfig
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
	microserviceState, err := state.NewMicroserviceStateAPIClient(conf.StateServiceAddress)
	if err != nil {
		panic(err)
	}

	geoFinder, err := amada.NewMaxMindIPGeoFinder(conf.MaxMindGeoFile, regions)
	if err != nil {
		panic(err)
	}

	// Modify used IP extraction method based on if in production vs debugging mode
	var extractor amada.RequestIPExtractor = amada.ExtractRequestIP
	if conf.DebuggingMode {
		extractor = amada.DebuggingExtractRequestIP
	}

	// Start APIs
	log.SetOutput(os.Stdout)
	go amada.StartServiceAPI(overrideListenAddr, microserviceState, geoFinder)
	amada.StartDeviceRoutingAPI(routeListenAddr, extractor, geoFinder, microserviceState,
		microserviceState, conf.PullDeciderAPIAddress)
}
