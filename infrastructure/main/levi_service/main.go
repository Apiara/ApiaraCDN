package main

import (
	"flag"
	"strconv"

	"github.com/Apiara/ApiaraCDN/infrastructure/levi"
	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

/*
Config Format
--------------
	# If no port is provided for gateway, gateway isn't started
	report_gateway_port = int
	content_gateway_port = int
	data_gateway_port = int

	stale_report_address = string
	session_report_address = string
	rule_change_address = string
	content_change_address = string
	data_access_address = string
*/

type leviConfig struct {
	// Ports to listen on for API Gateway
	ReportAPIListenPort     *int `toml:"report_gateway_port"`
	ContentAPIListenPort    *int `toml:"content_gateway_port"`
	DataAccessAPIListenPort *int `toml:"data_gateway_port"`

	// Addresses of internal APIs
	StaleReportAddress         string `toml:"stale_report_address"`
	SessionReportAddress       string `toml:"session_report_address"`
	RuleModificationAddress    string `toml:"rule_change_address"`
	ContentModificationAddress string `toml:"content_change_address"`
	DataAccessAddress          string `toml:"data_access_address"`
}

func main() {
	// Read configuration
	fnamePtr := flag.String("config", "", "TOML configuration file path")
	flag.Parse()

	var conf leviConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}

	// Start all specified gateways
	if conf.ReportAPIListenPort != nil {
		reportGatewayAddr := "" + strconv.Itoa(*conf.ReportAPIListenPort)
		go levi.StartReportAPI(reportGatewayAddr, conf.StaleReportAddress, conf.SessionReportAddress)
	}
	if conf.ContentAPIListenPort != nil {
		contentGatewayAddr := "" + strconv.Itoa(*conf.ContentAPIListenPort)
		go levi.StartContentAPI(contentGatewayAddr, conf.RuleModificationAddress, conf.ContentModificationAddress)
	}
	if conf.DataAccessAPIListenPort != nil {
		dataGatewayAddr := "" + strconv.Itoa(*conf.DataAccessAPIListenPort)
		go levi.StartDataAccessAPI(dataGatewayAddr, conf.DataAccessAddress)
	}

	// Pause main go routine
	select {}
}
