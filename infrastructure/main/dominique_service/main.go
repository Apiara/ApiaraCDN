package main

import (
	"flag"
	"strconv"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/dominique"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

/*
Config Format
--------------
  influxdb_address = string
  influxdb_token = string
  redis_address = string

  max_report_gap = time.Duration
  listen_port = int
*/

type dominiqueConfig struct {
	RedisDBAddress         string        `toml:"redis_address"`
	InfluxDBAddress        string        `toml:"influxdb_address"`
	InfluxDBToken          string        `toml:"influxdb_token"`
	ReportRetrievalTimeout time.Duration `toml:"max_report_gap"`
	Port                   int           `toml:"listen_port"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf dominiqueConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	listenAddr := ":" + strconv.Itoa(conf.Port)

	finder := dominique.NewRedisURLIndex(conf.RedisDBAddress)
	timeseries := dominique.NewInfluxTimeseriesDB(conf.InfluxDBAddress, conf.InfluxDBToken, finder)
	matcher := dominique.NewTimedSessionProcessor(conf.ReportRetrievalTimeout, timeseries)
	dominique.StartReportingAPI(listenAddr, matcher)
}
