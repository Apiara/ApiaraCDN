package main

import (
  "flag"
  "time"
  "strconv"
  "io/ioutil"
  "github.com/BurntSushi/toml"
  "github.com/Apiara/ApiaraCDN/infrastructure/dominique"
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

type config struct {
  RedisDBAddress string `toml:"redis_address"`
  InfluxDBAddress string `toml:"influxdb_address"`
  InfluxDBToken string `toml:"influxdb_token"`
  ReportRetrievalTimeout time.Duration `toml:"max_report_gap"`
  Port int `toml:"listen_port"`
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
  listenAddr := ":" + strconv.Itoa(conf.Port)

  finder := dominique.NewRedisURLIndex(conf.RedisDBAddress)
  timeseries := dominique.NewInfluxTimeseriesDB(conf.InfluxDBAddress, conf.InfluxDBToken, finder)
  matcher := dominique.NewTimedSessionProcessor(conf.ReportRetrievalTimeout, timeseries)
  dominique.StartReportingAPI(listenAddr, matcher)
}
