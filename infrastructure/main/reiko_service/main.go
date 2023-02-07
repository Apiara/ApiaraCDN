package main

import (
	"flag"
	"strconv"

	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
	"github.com/Apiara/ApiaraCDN/infrastructure/reiko"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
Config Format
--------------
redis_address = string
listen_port = int
*/

type reikoConfig struct {
	RedisDBAddress string `toml:"redis_address"`
	Port           int    `toml:"listen_port"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf reikoConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	listenAddr := ":" + strconv.Itoa(conf.Port)

	microserviceState, err := state.NewMicroserviceStateAPIClient(conf.RedisDBAddress)
	if err != nil {
		panic(err)
	}
	ruleset := reiko.NewPrefixContentRules(microserviceState)
	reiko.StartServiceAPI(listenAddr, ruleset)
}
