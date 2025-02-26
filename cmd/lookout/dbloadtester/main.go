package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/lookout/configuration"
	"github.com/armadaproject/armada/internal/lookout/dbloadtester"
)

const (
	customConfigLocation string = "config"
	customParamsLocation string = "params"
)

func init() {
	pflag.StringSlice(
		customConfigLocation,
		[]string{},
		"path to the configuration for the Lookout under test",
	)
	pflag.StringSlice(
		customParamsLocation,
		[]string{},
		"parameters to the load test, defining the shape of the load",
	)
	pflag.Parse()
}

func makeContext() (*armadacontext.Context, func()) {
	ctx := armadacontext.Background()
	ctx, cancel := armadacontext.WithCancel(ctx)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		signal.Stop(c)
		cancel()
	}
}

func main() {
	common.BindCommandlineArguments()

	var config configuration.LookoutConfig
	userSpecifiedConfigs := viper.GetStringSlice(customConfigLocation)
	common.LoadConfig(&config, "./config/lookout", userSpecifiedConfigs)

	var args dbloadtester.ReadTestConfig
	argsInput := viper.GetStringSlice(customParamsLocation)
	common.LoadConfig(&args, "./cmd/lookout/dbloadtester", argsInput)

	// Expose profiling endpoints if enabled.
	err := profiling.SetupPprof(config.Profiling, armadacontext.Background(), nil)
	if err != nil {
		log.Fatalf("Pprof setup failed, exiting, %v", err)
	}

	ctx, cleanup := makeContext()
	defer cleanup()

	log.SetLevel(log.DebugLevel)

	results, err := dbloadtester.DoQueries(config, args, ctx)
	if err != nil {
		log.Fatalf("error running queries, exiting, %v", err)
	}

	log.Infof("results: \n%v", results)
}
