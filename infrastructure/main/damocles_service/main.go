package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/damocles"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
Config Format
--------------
  devices_listen_port = int
  service_listen_port = int
  tracker_collection_duration = time.Duration
  state_address = string
*/

type damoclesConfig struct {
	RegionID                  string        `toml:"region_id"`
	DevicesAPIPort            int           `toml:"devices_listen_port"`
	ServiceAPIPort            int           `toml:"service_listen_port"`
	TrackerCollectionDuration time.Duration `toml:"tracker_collection_duration"`
	StateServiceAddress       string        `toml:"state_address"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	// Read configuration parameters
	var conf damoclesConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	deviceAPIAddr := ":" + strconv.Itoa(conf.DevicesAPIPort)
	serviceAPIAddr := ":" + strconv.Itoa(conf.ServiceAPIPort)

	// Create resources
	connections := damocles.NewEndpointConnectionManager()
	tracker := damocles.NewDesperationTracker(conf.TrackerCollectionDuration)
	updater := &categoryUpdater{connections, tracker}
	clientServicer := damocles.NewNeedClientServicer(connections, tracker)
	endpointAllocator := damocles.NewNeedEndpointAllocator(connections, tracker)

	// Sync damocles instance with what the network is expecting of it
	microserviceState, err := state.NewMicroserviceStateAPIClient(conf.StateServiceAddress)
	if err != nil {
		panic(err)
	}
	err = damocles.LoadCategories(conf.RegionID, microserviceState, updater)
	if err != nil {
		panic(err)
	}

	// Start services
	log.SetOutput(os.Stdout)
	go damocles.StartSignalingAPI(deviceAPIAddr, clientServicer, endpointAllocator)
	damocles.StartServiceAPI(serviceAPIAddr, updater)
}
