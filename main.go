// main.go
package main

import (
	"Snowballin/pkg/app"
	"Snowballin/utilities"
	"context"
	"fmt"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const banner = `
    _________                    ___.          .__  .__  .__     /\ 
   /   _____/ ____   ______  _  _\_ |__ _____  |  | |  | |__| ___)/ 
   \_____  \ /    \ /  _ \ \/ \/ /| __ \\__  \ |  | |  | |  |/    \ 
   /        \   |  (  <_> )     / | \_\ \/ __ \|  |_|  |_|  |   |  \
  /_______  /___|  /\____/ \/\_/  |___  (____  /____/____/__|___|  /
          \/     \/                   \/     \/                  \/ 

	Follow: @Ohmsecurities   -- A Hacker's Crypto DCA Bot
[]=========================================================================[]
`

// LoadConfig explicitly loads your AppConfig from JSON file using viper and creates Logger instance
func LoadConfig(path string) (utilities.AppConfig, *utilities.Logger, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("json") // Configuration file format
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return utilities.AppConfig{}, nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config utilities.AppConfig
	if err := viper.Unmarshal(&config); err != nil {
		return utilities.AppConfig{}, nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// --- NEW: Load and merge optimized parameters ---
	optParamsFile := "config/optimized_params.json"
	if _, err := os.Stat(optParamsFile); err == nil {
		viper.SetConfigFile(optParamsFile)
		if err := viper.MergeInConfig(); err != nil {
			fmt.Printf("Warning: could not merge optimized params: %v\n", err)
		} else {
			// Unmarshal again to overwrite indicator settings
			if err := viper.Unmarshal(&config); err != nil {
				return utilities.AppConfig{}, nil, fmt.Errorf("failed to unmarshal merged config: %w", err)
			}
			fmt.Println(">> Successfully loaded optimized indicator parameters.")
		}
	}

	// Explicit Logger instantiation based on loaded config logging level
	logLevel, err := utilities.ParseLogLevel(config.Logging.Level)
	if err != nil {
		return utilities.AppConfig{}, nil, fmt.Errorf("invalid log level in config: %w", err)
	}

	logger := utilities.NewLogger(logLevel)

	return config, logger, nil
}

func main() {
	// Explicitly print banner FIRST
	fmt.Println(banner)

	configPath := "config/config.json"

	cfg, logger, err := LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.LogWarn("Received signal: %v, initiating graceful shutdown.", sig)
		cancel()
	}()

	if err := app.Run(ctx, &cfg, logger); err != nil {
		logger.LogError("Application terminated with error: %v", err)
		os.Exit(1)
	}

	logger.LogInfo("Snowballin shutdown complete at %s", time.Now().Format(time.RFC1123))
}
