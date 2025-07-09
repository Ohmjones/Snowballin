package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"Snowballin/pkg/app"
	"Snowballin/utilities"
)

const banner = `
    _________                    ___.          .__  .__  .__     /\ 
   /   _____/ ____   ______  _  _\_ |__ _____  |  | |  | |__| ___)/ 
   \_____  \ /    \ /  _ \ \/ \/ /| __ \\__  \ |  | |  | |  |/    \ 
   /        \   |  (  <_> )     / | \_\ \/ __ \|  |_|  |_|  |   |  \
  /_______  /___|  /\____/ \/\_/  |___  (____  /____/____/__|___|  /
          \/     \/                   \/     \/                  \/ 

	Follow: @Ohmsecurities   -- A Hacker's Guide to Cryptocurrency'
[]=========================================================================[]
`

var (
	cfgFile string
	cfg     utilities.AppConfig
	logger  *utilities.Logger
)

// rootCmd represents the base command for the Snowballin CLI.
var rootCmd = &cobra.Command{
	Use:   "snowballin",
	Short: "Snowballin automated trading bot",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Load config
		viper.SetConfigFile(cfgFile)
		viper.AutomaticEnv()
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("failed to read config: %w", err)
		}
		if err := viper.Unmarshal(&cfg); err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}

		// Initialize logger
		level, err := utilities.ParseLogLevel(cfg.Logging.Level)
		if err != nil {
			return fmt.Errorf("invalid log level: %w", err)
		}
		logger = utilities.NewLogger(level)
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		return app.Run(ctx, &cfg, logger)
	},
}

// Execute runs the root command.
func Execute() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "config/config.json", "config file (default is config/config.json)")
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
