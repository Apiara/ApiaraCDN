package main

import (
	"flag"
	"strconv"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/cyprus"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

/*
Config Format
--------------
media_formats = [ ".mp4", ".mov", ...]
processing_dir = "../workingdir/"
publishing_dir = "../publish/"
aes_key_size = 16 | 24 | 32
redis_address = addr
processing_listen_port = int
storage_listen_port = int
*/

type cyprusConfig struct {
	MediaFormats      []string `toml:"media_formats"`
	ProcessingDir     string   `toml:"processing_dir"`
	PublishingDir     string   `toml:"publishing_dir"`
	AESKeySize        int      `toml:"aes_key_size"`
	RedisDBAddress    string   `toml:"redis_address"`
	ProcessingAPIPort int      `toml:"processing_listen_port"`
	StorageAPIPort    int      `toml:"storage_listen_port"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf cyprusConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	processingListenAddr := ":" + strconv.Itoa(conf.ProcessingAPIPort)
	storageListenAddr := ":" + strconv.Itoa(conf.StorageAPIPort)

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
	dataIndex := infra.NewRedisDataIndex(conf.RedisDBAddress)
	storage, err := cyprus.NewFilesystemStorageManager(conf.PublishingDir, dataIndex)
	if err != nil {
		panic(err)
	}

	// Run
	go cyprus.StartDataProcessingAPI(processingListenAddr, preprocessor, processor, storage)
	cyprus.StartStorageAPI(storageListenAddr, conf.PublishingDir)
}
