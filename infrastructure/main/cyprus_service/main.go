package main

import (
  "flag"
  "strconv"
  "io/ioutil"
  "github.com/BurntSushi/toml"
  "github.com/Apiara/ApiaraCDN/infrastructure/cyprus"
)

/*
Config Format
--------------
media_formats = [ ".mp4", ".mov", ...]
processing_dir = "../workingdir/"
publishing_dir = "../publish/"
aes_key_size = 16 | 24 | 32
redis_address = addr
listen_port = int
*/

type config struct {
  MediaFormats []string `toml:"media_formats"`
  ProcessingDir string `toml:"processing_dir"`
  PublishingDir string `toml:"publishing_dir"`
  AESKeySize int `toml:"aes_key_size"`
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

  // Create preprocessor
  rawPreprocessor := cyprus.NewRawPreprocessor(conf.ProcessingDir)
  hlsPreprocessor := cyprus.NewHLSPreprocessor(conf.ProcessingDir)
  preprocessorMap := make(map[string]cyprus.DataPreprocessor)
  preprocessorMap[".m3u8"] = hlsPreprocessor
  for _, ext := range conf.MediaFormats {
    preprocessorMap[ext] = rawPreprocessor
  }
  preprocessor := cyprus.NewCompoundPreprocessor(preprocessorMap)

  // Create processor
  processor, err := cyprus.NewAESDataProcessor(conf.AESKeySize, conf.ProcessingDir)
  if err != nil {
    panic(err)
  }

  // Create storage manager
  storage, err := cyprus.NewRedisStorageManager(conf.RedisDBAddress, conf.PublishingDir)
  if err != nil {
    panic(err)
  }

  // Run
  cyprus.StartDataProcessingAPI(listenAddr, preprocessor, processor, storage)
}
