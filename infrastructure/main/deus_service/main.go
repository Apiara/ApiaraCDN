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

service_listen_port = int

validate_api = string
process_api = string
coordinate_api = string

redis_address = string
*/

type (
	deusConfig struct {
		MediaFormats         []string      `toml:"media_formats"`
		PullFrequency        time.Duration `toml:"pull_frequency"`
		PullRequestThreshold int           `toml:"pull_request_threshold"`
		ServiceListenPort    int           `toml:"service_listen_port"`
		InternalDataAddr     string        `toml:"internal_data_addr"`
		ProcessingDir        string        `toml:"processing_dir"`
		ValidateAPIAddress   string        `toml:"validate_api"`
		ProcessAPIAddress    string        `toml:"process_api"`
		CoordinateAPIAddress string        `toml:"coordinate_api"`
		RedisAddress         string        `toml:"redis_address"`
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

	serviceListenAddr := ":" + strconv.Itoa(conf.ServiceListenPort)

	// Create resources
	dataIndex := infra.NewRedisDataIndex(conf.RedisAddress)
	contentState := deus.NewRedisContentLocationIndex(conf.RedisAddress)
	validator, err := deus.NewContentValidatorClient(conf.ValidateAPIAddress)
	if err != nil {
		panic(err)
	}

	manager, err := deus.NewMasterContentManager(contentState, dataIndex,
		conf.ProcessAPIAddress, conf.CoordinateAPIAddress)
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
	deus.StartServiceAPI(serviceListenAddr, staleChecker, contentState, pullDecider, manager)
}
