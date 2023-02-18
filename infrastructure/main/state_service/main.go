package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
Config Format
--------------
redis_address = string
listen_port = int
*/

type stateConfig struct {
	RedisDBAddress string `toml:"redis_address"`
	Port           int    `toml:"listen_port"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf stateConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	listenAddr := ":" + strconv.Itoa(conf.Port)

	manager := state.NewRedisMicroserviceState(conf.RedisDBAddress)

	// Start service
	log.SetOutput(os.Stdout)
	state.StartDataService(listenAddr, manager)
}
