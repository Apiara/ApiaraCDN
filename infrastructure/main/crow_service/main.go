package main

import (
	"flag"
	"strconv"

	"github.com/Apiara/ApiaraCDN/infrastructure/crow"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

/*
Config Format
--------------
size_classes = [int, int, ...]
service_listen_port = int
allocator_listen_port = int
*/

type crowConfig struct {
	SizeClasses   []int64 `toml:"size_classes"`
	ServicePort   int     `toml:"service_listen_port"`
	AllocatorPort int     `toml:"allocator_listen_port"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf crowConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}

	allocatorAddr := ":" + strconv.Itoa(conf.AllocatorPort)
	serviceAddr := ":" + strconv.Itoa(conf.ServicePort)
	allocator := crow.NewEvenDataAllocator(conf.SizeClasses)

	go crow.StartDataAllocatorAPI(allocatorAddr, allocator)
	crow.StartServiceAPI(serviceAddr, allocator)
}
