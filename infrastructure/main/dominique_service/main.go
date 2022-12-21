package main

import (
	"flag"
	"strconv"
	"time"

	infra "github.com/Apiara/ApiaraCDN/infrastructure"
	"github.com/Apiara/ApiaraCDN/infrastructure/dominique"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

/*
Config Format
--------------
  influxdb_address = string
  influxdb_token = string
  redis_address = string

  postgres_host = string
  postgres_port = int
  postgres_username = string
  postgres_password = string
  postgres_dbname = string

  max_report_variability_bytes = int
  batch_remediation_frequency = time.Duration
  max_report_gap = time.Duration
  listen_port = int
*/

type dominiqueConfig struct {
	RedisDBAddress              string        `toml:"redis_address"`
	InfluxDBAddress             string        `toml:"influxdb_address"`
	InfluxDBToken               string        `toml:"influxdb_token"`
	PostgresHost                string        `toml:"postgres_host"`
	PostgresPort                int           `toml:"postgres_port"`
	PostgresUsername            string        `toml:"postgres_username"`
	PostgresPassword            string        `toml:"postgres_password"`
	PostgresDatabase            string        `toml:"postgres_database"`
	AcceptableReportVariability int64         `toml:"max_report_variability_bytes"`
	ReportRetrievalTimeout      time.Duration `toml:"max_report_gap"`
	BatchRemediationFrequency   time.Duration `toml:"batch_remediation_frequency"`
	Port                        int           `toml:"listen_port"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf dominiqueConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	listenAddr := ":" + strconv.Itoa(conf.Port)

	// Create resources
	dataIndex := infra.NewRedisDataIndex(conf.RedisDBAddress)
	timeseries := dominique.NewInfluxTimeseriesDB(conf.InfluxDBAddress, conf.InfluxDBToken, dataIndex)
	matcher := dominique.NewTimedSessionProcessor(conf.ReportRetrievalTimeout, timeseries)
	remediationQueue, err := dominique.NewPostgresRemediationQueue(conf.PostgresHost, conf.PostgresPort,
		conf.PostgresUsername, conf.PostgresPassword, conf.PostgresDatabase)
	if err != nil {
		panic(err)
	}
	remediators := []dominique.Remediator{
		dominique.NewTimeframeRemediator(),
		dominique.NewByteOffsetRemediator(conf.AcceptableReportVariability),
	}

	// Start core reporting service
	go dominique.StartReportingAPI(listenAddr, matcher)

	// Start batch remediation service
	dominique.StartRemediaton(conf.BatchRemediationFrequency, timeseries, remediators, remediationQueue)
}
