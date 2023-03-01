package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/dominique"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
	"github.com/Apiara/ApiaraCDN/infrastructure/state"
)

/*
Config Format
--------------
  influxdb_address = string
  influxdb_token = string
  state_address = string

  postgres_host = string
  postgres_port = int
  postgres_username = string
  postgres_password = string
  postgres_dbname = string

  max_report_variability_bytes = int
  batch_remediation_frequency = time.Duration
  max_report_gap = time.Duration

  report_listen_port = int
  service_listen_port = int
*/

type dominiqueConfig struct {
	StateServiceAddress         string        `toml:"state_address"`
	InfluxDBAddress             string        `toml:"influxdb_address"`
	InfluxDBToken               string        `toml:"influxdb_token"`
	PostgresHost                string        `toml:"postgres_host"`
	PostgresPort                int           `toml:"postgres_port"`
	PostgresUsername            string        `toml:"postgres_username"`
	PostgresPassword            string        `toml:"postgres_password"`
	PostgresDatabase            string        `toml:"postgres_dbname"`
	AcceptableReportVariability int64         `toml:"max_report_variability_bytes"`
	ReportRetrievalTimeout      time.Duration `toml:"max_report_gap"`
	BatchRemediationFrequency   time.Duration `toml:"batch_remediation_frequency"`
	ReportListenPort            int           `toml:"report_listen_port"`
	ServiceListenPort           int           `toml:"service_listen_port"`
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf dominiqueConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	serviceAddr := ":" + strconv.Itoa(conf.ServiceListenPort)
	reportAddr := ":" + strconv.Itoa(conf.ReportListenPort)

	// Create resources
	microserviceState, err := state.NewMicroserviceStateAPIClient(conf.StateServiceAddress)
	if err != nil {
		panic(err)
	}
	timeseries := dominique.NewInfluxTimeseriesDB(conf.InfluxDBAddress, conf.InfluxDBToken,
		conf.ReportRetrievalTimeout, microserviceState)
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

	// Start APIs
	go dominique.StartReportingAPI(reportAddr, matcher)
	go dominique.StartDataAccessAPI(serviceAddr, timeseries)

	// Start batch remediation service
	log.SetOutput(os.Stdout)
	dominique.StartRemediaton(conf.BatchRemediationFrequency, timeseries, remediators, remediationQueue)
}
