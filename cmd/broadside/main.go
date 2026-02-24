package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"

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
	teardownOnlyFlag     string = "teardown-only"
	teardownTimeout             = 2 * time.Minute
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
	pflag.Bool(
		teardownOnlyFlag,
		false,
		"Connect to the configured database and tear it down, then exit. Use to clean up after a crashed load test.")
	pflag.Parse()
}

// resolveYAMLMergeKeys reads a YAML file and resolves any merge keys (<<)
// and anchors/aliases, which Viper does not natively support. The yaml.v3
// library handles parsing and alias resolution, but does not apply merge key
// semantics (a YAML 1.1 feature) when decoding to interface{}.
func resolveYAMLMergeKeys(path string) (*bytes.Reader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var raw interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	applyMergeKeys(raw)
	resolved, err := yaml.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("re-marshalling %s: %w", path, err)
	}
	return bytes.NewReader(resolved), nil
}

// applyMergeKeys recursively walks a decoded YAML tree and applies merge key
// (<<) semantics: keys from the referenced mapping are merged into the parent
// mapping without overwriting existing keys.
func applyMergeKeys(v interface{}) {
	switch val := v.(type) {
	case map[string]interface{}:
		for _, child := range val {
			applyMergeKeys(child)
		}
		mergeVal, ok := val["<<"]
		if !ok {
			return
		}
		delete(val, "<<")
		mergeMappingInto(val, mergeVal)
	case []interface{}:
		for _, item := range val {
			applyMergeKeys(item)
		}
	}
}

// mergeMappingInto merges keys from src into dst without overwriting. src may
// be a single mapping or a sequence of mappings, per the YAML merge key spec.
func mergeMappingInto(dst map[string]interface{}, src interface{}) {
	switch s := src.(type) {
	case map[string]interface{}:
		for k, v := range s {
			if _, exists := dst[k]; !exists {
				dst[k] = v
			}
		}
	case []interface{}:
		for _, item := range s {
			if m, ok := item.(map[string]interface{}); ok {
				for k, v := range m {
					if _, exists := dst[k]; !exists {
						dst[k] = v
					}
				}
			}
		}
	}
}

func loadConfig(configFiles []string) configuration.TestConfig {
	if len(configFiles) == 0 {
		logging.Fatal("No configuration file specified. Use --config flag to specify a configuration file.")
	}

	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigType("yaml")

	// Load the first config file, resolving YAML merge keys
	reader, err := resolveYAMLMergeKeys(configFiles[0])
	if err != nil {
		logging.Fatalf("Error reading config from %s: %v", configFiles[0], err)
	}
	if err := v.ReadConfig(reader); err != nil {
		logging.Fatalf("Error reading config from %s: %v", configFiles[0], err)
	}
	logging.Infof("Read config from %s", configFiles[0])

	// Merge in any additional config files
	for i := 1; i < len(configFiles); i++ {
		reader, err := resolveYAMLMergeKeys(configFiles[i])
		if err != nil {
			logging.Fatalf("Error merging config from %s: %v", configFiles[i], err)
		}
		if err := v.MergeConfig(reader); err != nil {
			logging.Fatalf("Error merging config from %s: %v", configFiles[i], err)
		}
		logging.Infof("Merged config from %s", configFiles[i])
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

	if viper.GetBool(teardownOnlyFlag) {
		database, err := orchestrator.NewDatabase(config)
		if err != nil {
			logging.Fatalf("creating database: %v", err)
		}
		teardownCtx, teardownCancel := context.WithTimeout(context.Background(), teardownTimeout)
		defer teardownCancel()
		if err := database.InitialiseSchema(teardownCtx); err != nil {
			logging.Fatalf("initialising database schema: %v", err)
		}
		if err := database.TearDown(teardownCtx); err != nil {
			logging.Fatalf("tearing down database: %v", err)
		}
		logging.Info("Database teardown complete.")
		database.Close()
		return
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		select {
		case <-sigCh:
			logging.Info("Interrupt received, cleaning up...")
			cancel()
		case <-ctx.Done():
		}
	}()

	if err := orchestrator.NewRunner(config, resultsDir).Run(ctx); err != nil {
		panic(err)
	}
}
