package main

import (
	"flag"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/damocles"
	"github.com/BurntSushi/toml"
)

/*
Config Format
--------------
  devices_listen_port = int
  service_listen_port = int
  tracker_collection_duration = time.Duration
*/

type config struct {
	DevicesAPIPort            int           `toml:"devices_listen_port"`
	ServiceAPIPort            int           `toml:"service_listen_port"`
	TrackerCollectionDuration time.Duration `toml:"tracker_collection_duration"`
}

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
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	conf, err := readConfig(*fnamePtr)
	if err != nil {
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
