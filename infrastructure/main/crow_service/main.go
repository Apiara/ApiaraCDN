package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/crow"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
Config Format
--------------
size_classes = [int, int, ...]
service_listen_port = int
allocator_listen_port = int

state_address = string
*/

type crowConfig struct {
	SizeClasses             []int64       `toml:"size_classes"`
	ServicePort             int           `toml:"service_listen_port"`
	AllocatorPort           int           `toml:"allocator_listen_port"`
	StateServiceAddress     string        `toml:"state_address"`
	AllocatorPrecomputeFreq time.Duration `toml:"precompute_frequency"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf crowConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}

	// Create resources
	allocatorAddr := ":" + strconv.Itoa(conf.AllocatorPort)
	serviceAddr := ":" + strconv.Itoa(conf.ServicePort)

	microserviceState, err := state.NewMicroserviceStateAPIClient(conf.StateServiceAddress)
	if err != nil {
		panic(err)
	}

	// Creator content allocator
	allocatorConstructor := func(region string) (crow.DataAllocator, error) {
		edgeServerAddr, err := microserviceState.GetServerPrivateAddress(region)
		if err != nil {
			return nil, err
		}
		alloc, err := crow.NewPrecomputedDataAllocator(edgeServerAddr, conf.AllocatorPrecomputeFreq, conf.SizeClasses)
		if err != nil {
			return nil, err
		}
		return alloc, nil
	}
	allocator := crow.NewCompoundLocationDataAllocator(conf.SizeClasses, allocatorConstructor)

	// Sync crow state with what network expects of it
	if err = crow.LoadContent(microserviceState, allocator); err != nil {
		panic(err)
	}

	// Start service APIs
	log.SetOutput(os.Stdout)
	go crow.StartDataAllocatorAPI(allocatorAddr, allocator)
	crow.StartServiceAPI(serviceAddr, allocator)
}
