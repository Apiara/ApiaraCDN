package main

import (
	"flag"
	"strconv"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/damocles"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

/*
Config Format
--------------
  devices_listen_port = int
  service_listen_port = int
  tracker_collection_duration = time.Duration
*/

type damoclesConfig struct {
	DevicesAPIPort            int           `toml:"devices_listen_port"`
	ServiceAPIPort            int           `toml:"service_listen_port"`
	TrackerCollectionDuration time.Duration `toml:"tracker_collection_duration"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf damoclesConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	deviceAPIAddr := ":" + strconv.Itoa(conf.DevicesAPIPort)
	serviceAPIAddr := ":" + strconv.Itoa(conf.ServiceAPIPort)

	connections := damocles.NewEndpointConnectionManager()
	tracker := damocles.NewDesperationTracker(conf.TrackerCollectionDuration)
	updater := &categoryUpdater{connections, tracker}

	clientServicer := damocles.NewNeedClientServicer(connections, tracker)
	endpointAllocator := damocles.NewNeedEndpointAllocator(connections, tracker)
	go damocles.StartSignalingAPI(deviceAPIAddr, clientServicer, endpointAllocator)
	damocles.StartDamoclesServiceAPI(serviceAPIAddr, updater)
}
