package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

type replConfig struct {
	RoutePort            int    `toml:"route_port"`
	AllocatorPort        int    `toml:"allocator_port"`
	ContentManagerPort   int    `toml:"content_manager_port"`
	RuleManagerPort      int    `toml:"rule_manager_port"`
	RegionManagerPort    int    `toml:"region_manager_port"`
	ReportAPIPort        int    `toml:"report_api_port"`
	StatQueryPort        int    `toml:"stat_query_port"`
	FileServerListenPort int    `toml:"fs_listen_port"`
	FileServerDirectory  string `toml:"fs_directory"`
}

func startREPL(actionMap map[string]action) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("ApiaraCDN Integration Testing REPL")
	fmt.Println("----------------------------------")

	for {
		fmt.Printf("> ")
		input, _ := reader.ReadString('\n')
		args := strings.Split(strings.Trim(input, " \t\n"), " ")

		if action, ok := actionMap[strings.ToLower(args[0])]; ok {
			response, err := action(args[1:])
			if err != nil {
				fmt.Printf("Error: %s\n", err.Error())
			} else {
				fmt.Println(response)
			}
		} else {
			fmt.Println("Error: invalid command")
		}
	}
}

func main() {
	fnamePtr := flag.String("config", "", "TOML configuration file")
	flag.Parse()

	var conf replConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}

	actionMap := createActionMap(conf)
	go serveDirectory(conf.FileServerListenPort, conf.FileServerDirectory)
	fmt.Printf("[*] Hosting test content on port %d\n", conf.FileServerListenPort)
	startREPL(actionMap)
}
