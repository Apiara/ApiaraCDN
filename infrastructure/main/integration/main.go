package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"

	"github.com/Apiara/ApiaraCDN/infrastructure/main/config"
)

// Master configuration used for integration build+boot pipeline
type integrationConfig struct {
	// General configuration parameters
	StartRoot string `toml:"run_directory"`
	LogRoot   string `toml:"log_directory"`

	// Prelim Service Configs
	StateConfig string `toml:"state_config"`

	// Edge service configs
	DamoclesConfig string `toml:"damocles_config"`
	CrowConfig     string `toml:"crow_config"`

	// Core service configs
	ReikoConfig     string `toml:"reiko_config"`
	AmadaConfig     string `toml:"amada_config"`
	CyprusConfig    string `toml:"cyprus_config"`
	DeusConfig      string `toml:"deus_config"`
	DominiqueConfig string `toml:"dominique_config"`

	// Gateway service config
	LeviConfig string `toml:"levi_config"`
}

// Prepares compiled binaries and logging directories
func prepareExecutionEnvironment(binDir string, logDir string) {
	os.RemoveAll(logDir)
	os.RemoveAll(binDir)
	if err := os.Mkdir(binDir, os.ModePerm); err != nil {
		panic(fmt.Errorf("failed to setup root execution directory(%s): %w", binDir, err))
	}
	if err := os.Mkdir(logDir, os.ModePerm); err != nil {
		panic(fmt.Errorf("failed to setup log directory(%s): %w", logDir, err))
	}
}

// Builds go services
func buildServices(buildMap map[string]string, outDir string) {
	baseArgs := []string{"build", "-o"}

	for serviceName, mainFile := range buildMap {
		outPath := path.Join(outDir, serviceName)
		args := append(baseArgs, outPath, mainFile)
		fmt.Printf("[*] Building service %s... ", serviceName)
		if err := exec.Command("go", args...).Run(); err != nil {
			panic(fmt.Errorf("failed to build service(%s): %w", serviceName, err))
		}
		fmt.Printf("✓\n")
	}
}

// Starts go services stored in rootDir. Writes log outputs to logRoot
func startServices(serviceBinaries map[string]string, rootDir string, logRoot string) {
	for service, configFile := range serviceBinaries {
		time.Sleep(time.Second / 2)
		fname := path.Join(rootDir, service)
		cmd := exec.Command(fname, "-config", configFile)
		go func(serviceName string, command *exec.Cmd) {
			// Merge process outputs
			stdout, err := command.StdoutPipe()
			if err != nil {
				panic(err)
			}
			stderr, err := command.StderrPipe()
			if err != nil {
				panic(err)
			}

			// Create logging output
			output := io.MultiReader(stdout, stderr)
			scanner := bufio.NewScanner(output)
			logFile, err := os.Create(path.Join(logRoot, serviceName+".log"))
			if err != nil {
				panic(err)
			}

			// Run command
			fmt.Printf("[*] Starting service %s... ", serviceName)
			if err = command.Start(); err != nil {
				fmt.Printf("failed\n")
				panic(err)
			}
			fmt.Printf("writing output to %s\n", logFile.Name())

			// Forward all output ot log file
			for scanner.Scan() {
				msg := scanner.Text()
				if _, err = logFile.WriteString(msg); err != nil {
					panic(err)
				}
			}
			if err = command.Wait(); err != nil {
				panic(err)
			}
		}(service, cmd)
	}
}

// Convert filepath.Abs from (string, error) -> string return type
func absolutePath(file string) string {
	absPath, err := filepath.Abs(file)
	if err != nil {
		panic(err)
	}
	return absPath
}

func main() {
	// Read master configuration
	fnamePtr := flag.String("config", "", "TOML configuration file")
	flag.Parse()

	var conf integrationConfig
	if err := config.ReadTOMLConfig(*fnamePtr, &conf); err != nil {
		panic(err)
	}
	executionRoot := absolutePath(conf.StartRoot)
	logRoot := absolutePath(conf.LogRoot)

	// Prepare working directories
	prepareExecutionEnvironment(executionRoot, logRoot)

	// Build microservices
	buildMap := map[string]string{
		"amada":     absolutePath("../amada_service"),
		"crow":      absolutePath("../crow_service"),
		"cyprus":    absolutePath("../cyprus_service"),
		"damocles":  absolutePath("../damocles_service"),
		"deus":      absolutePath("../deus_service"),
		"dominique": absolutePath("../dominique_service"),
		"levi":      absolutePath("../levi_service"),
		"reiko":     absolutePath("../reiko_service"),
		"state":     absolutePath("../state_service"),
	}
	buildServices(buildMap, executionRoot)

	// Run microservices
	prelimServices := map[string]string{"state": absolutePath(conf.StateConfig)}
	edgeServices := map[string]string{
		"damocles": absolutePath(conf.DamoclesConfig),
		"crow":     absolutePath(conf.CrowConfig),
	}
	coreServices := map[string]string{
		"dominique": absolutePath(conf.DominiqueConfig),
		"cyprus":    absolutePath(conf.CyprusConfig),
		"reiko":     absolutePath(conf.ReikoConfig),
		"deus":      absolutePath(conf.DeusConfig),
		"amada":     absolutePath(conf.AmadaConfig),
	}

	startServices(prelimServices, executionRoot, logRoot)
	startServices(edgeServices, executionRoot, logRoot)
	startServices(coreServices, executionRoot, logRoot)

	// Wait indefinetly
	end := make(chan struct{})
	<-end
}
