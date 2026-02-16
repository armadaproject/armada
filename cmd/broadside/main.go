package main

import (
	"context"
	"fmt"
	"strings"

	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/broadside/configuration"
	"github.com/armadaproject/armada/internal/broadside/estimation"
	"github.com/armadaproject/armada/internal/broadside/orchestrator"
	"github.com/armadaproject/armada/internal/common"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/logging"
)

const (
	customConfigLocation string = "config"
	resultsDirFlag       string = "results-dir"
)

func init() {
	pflag.StringSlice(
		customConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file")
	pflag.String(
		resultsDirFlag,
		"",
		"Directory to write result files to (defaults to cmd/broadside/results)")
	pflag.Parse()
}

func loadConfig(configFiles []string) configuration.TestConfig {
	if len(configFiles) == 0 {
		logging.Fatal("No configuration file specified. Use --config flag to specify a configuration file.")
	}

	v := viper.NewWithOptions(viper.KeyDelimiter("::"))

	// Load the first config file
	v.SetConfigFile(configFiles[0])
	if err := v.ReadInConfig(); err != nil {
		logging.Fatalf("Error reading config from %s: %v", configFiles[0], err)
	}
	logging.Infof("Read config from %s", v.ConfigFileUsed())

	// Merge in any additional config files
	for i := 1; i < len(configFiles); i++ {
		v.SetConfigFile(configFiles[i])
		if err := v.MergeInConfig(); err != nil {
			logging.Fatalf("Error merging config from %s: %v", configFiles[i], err)
		}
		logging.Infof("Merged config from %s", v.ConfigFileUsed())
	}

	// Environment variable support
	v.SetEnvKeyReplacer(strings.NewReplacer("::", "_"))
	v.SetEnvPrefix("ARMADA")
	v.AutomaticEnv()

	// Unmarshal with custom hooks and metadata tracking
	var config configuration.TestConfig
	var metadata mapstructure.Metadata
	customHooks := append(slices.Clone(commonconfig.CustomHooks), func(c *mapstructure.DecoderConfig) { c.Metadata = &metadata })
	if err := v.Unmarshal(&config, customHooks...); err != nil {
		logging.Fatal(err)
	}

	// Log config keys
	if len(metadata.Keys) > 0 {
		logging.Debugf("Decoded keys: %v", metadata.Keys)
	}
	if len(metadata.Unused) > 0 {
		logging.Warnf("Unused keys: %v", metadata.Unused)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		logging.Fatalf("Configuration validation failed: %v", err)
	}

	return config
}

func main() {
	common.BindCommandlineArguments()

	userSpecifiedConfigs := viper.GetStringSlice(customConfigLocation)
	config := loadConfig(userSpecifiedConfigs)

	est := estimation.Estimate(config)
	if estimation.ShouldPrompt(est) {
		confirmed, err := estimation.DisplayEstimationAndConfirm(est)
		if err != nil {
			panic(err)
		}
		if !confirmed {
			fmt.Println("Test cancelled by user")
			return
		}
	}

	resultsDir := viper.GetString(resultsDirFlag)
	if err := orchestrator.NewRunner(config, resultsDir).Run(context.Background()); err != nil {
		panic(err)
	}
}
