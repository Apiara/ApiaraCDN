package main

import (
	"flag"
	"strconv"
	"time"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/cyprus"
	"github.com/Apiara/ApiaraCDN/infrastructure/deus"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

/*
Configuration Format
--------------------

media_formats = [ ".mp4", ".mov" ]
pull_frequency = time.Duration
pull_request_threshold = int
processing_dir = string
internal_data_addr = string

override_listen_port = int
route_listen_port = int
stale_report_listen_port = int
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
	deusConfig struct {
		MediaFormats          []string      `toml:"media_formats"`
		PullFrequency         time.Duration `toml:"pull_frequency"`
		PullRequestThreshold  int           `toml:"pull_request_threshold"`
		OverrideListenPort    int           `toml:"override_listen_port"`
		RouteListenPort       int           `toml:"route_listen_port"`
		StaleReportListenPort int           `toml:"stale_report_listen_port"`
		InternalDataAddr      string        `toml:"internal_data_addr"`
		ProcessingDir         string        `toml:"processing_dir"`
		MaxMindGeoFile        string        `toml:"mmdb_geo_file"`
		ValidateAPIAddress    string        `toml:"validate_api"`
		ProcessAPIAddress     string        `toml:"process_api"`
		CoordinateAPIAddress  string        `toml:"coordinate_api"`
		RedisAddress          string        `toml:"redis_address"`
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
	regions := []deus.Region{}
	for name, region := range conf.Regions {
		regions = append(regions, deus.Region{
			Name:         name,
			MinLatitude:  region.MinLatitude,
			MaxLatitude:  region.MaxLatitude,
			MinLongitude: region.MinLongitude,
			MaxLongitude: region.MaxLongitude,
		})
	}

	overrideListenAddr := ":" + strconv.Itoa(conf.OverrideListenPort)
	routeListenAddr := ":" + strconv.Itoa(conf.RouteListenPort)
	staleListenAddr := ":" + strconv.Itoa(conf.StaleReportListenPort)

	// Create resources
	dataIndex := infra.NewRedisDataIndex(conf.RedisAddress)
	contentState := deus.NewRedisContentLocationIndex(conf.RedisAddress)
	serverIndex := deus.NewRedisGeoServerIndex(conf.RedisAddress)
	validator, err := deus.NewContentValidatorClient(conf.ValidateAPIAddress)
	if err != nil {
		panic(err)
	}

	manager, err := deus.NewMasterContentManager(contentState, dataIndex,
		conf.ProcessAPIAddress, conf.CoordinateAPIAddress)
	if err != nil {
		panic(err)
	}

	geoFinder, err := deus.NewMaxMindIPGeoFinder(conf.MaxMindGeoFile, regions)
	if err != nil {
		panic(err)
	}
	pullDecider := deus.NewThresholdPullDecider(validator, manager, contentState,
		conf.PullRequestThreshold, conf.PullFrequency)

	// Create preprocessor
	rawPreprocessor := cyprus.NewRawPreprocessor(conf.ProcessingDir)
	hlsPreprocessor := cyprus.NewHLSPreprocessor(conf.ProcessingDir)
	preprocessorMap := make(map[string]cyprus.DataPreprocessor)
	preprocessorMap[".m3u8"] = hlsPreprocessor
	for _, ext := range conf.MediaFormats {
		preprocessorMap[ext] = rawPreprocessor
	}
	preprocessor := cyprus.NewCompoundPreprocessor(preprocessorMap)

	// Create stale data checker
	staleChecker, err := deus.NewChecksumDataValidator(conf.InternalDataAddr, preprocessor, dataIndex)

	// Start APIs
	go deus.StartStaleReadReportAPI(staleListenAddr, staleChecker, contentState, manager)
	go deus.StartServiceAPI(overrideListenAddr, manager, serverIndex, geoFinder)
	deus.StartDeviceRoutingAPI(routeListenAddr, geoFinder, contentState,
		serverIndex, pullDecider)
}
