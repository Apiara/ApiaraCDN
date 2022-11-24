package main

import (
  "github.com/Apiara/ApiaraCDN/infrastructure/reiko"
  "github.com/BurntSushi/toml"
  "io/ioutil"
  "strconv"
  "flag"
)
/*
Config Format
--------------
redis_address = string
listen_port = int
*/

type config struct {
  RedisDBAddress string `toml:"redis_address"`
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

  ruleManager := reiko.NewPrefixContentRules(conf.RedisDBAddress)
  reiko.StartRulesetAPI(listenAddr, ruleManager)
}
