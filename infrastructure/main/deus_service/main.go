package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/cyprus"
	"github.com/Apiara/ApiaraCDN/infrastructure/deus"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
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

state_address = string
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
		StateServiceAddress  string        `toml:"state_address"`
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
	microserviceState, err := state.NewMicroserviceStateAPIClient(conf.StateServiceAddress)
	if err != nil {
		panic(err)
	}
	validator, err := deus.NewContentValidatorClient(conf.ValidateAPIAddress)
	if err != nil {
		panic(err)
	}

	manager, err := deus.NewMasterContentManager(microserviceState, microserviceState,
		conf.ProcessAPIAddress, conf.CoordinateAPIAddress)
	if err != nil {
		panic(err)
	}

	pullDecider := deus.NewThresholdPullDecider(validator, manager, microserviceState,
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
	staleChecker, err := deus.NewChecksumDataValidator(conf.InternalDataAddr, preprocessor, microserviceState)
	if err != nil {
		panic(err)
	}

	// Start APIs
	log.SetOutput(os.Stdout)
	deus.StartServiceAPI(serviceListenAddr, staleChecker, microserviceState, pullDecider, manager)
}
