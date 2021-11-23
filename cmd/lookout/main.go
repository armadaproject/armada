package main

import (
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/G-Research/armada/internal/lookout/repository"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/common/health"
	"github.com/G-Research/armada/internal/common/serve"
	"github.com/G-Research/armada/internal/lookout"
	"github.com/G-Research/armada/internal/lookout/configuration"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	lookoutApi "github.com/G-Research/armada/pkg/api/lookout"
)

const CustomConfigLocation string = "config"
const MigrateDatabase string = "migrateDatabase"
const pruneDatabase = "pruneDatabase"

func init() {
	pflag.StringSlice(CustomConfigLocation, []string{}, "Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)")
	pflag.Bool(MigrateDatabase, false, "Migrate database instead of running server")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.LookoutConfiguration
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	common.LoadConfig(&config, "./config/lookout", userSpecifiedConfigs)

	if viper.GetBool(MigrateDatabase) {
		db, err := postgres.Open(config.Postgres)
		if err != nil {
			panic(err)
		}

		err = schema.UpdateDatabase(db)
		if err != nil {
			panic(err)
		}
		os.Exit(0)
	}

	if viper.GetBool(pruneDatabase) {
		db, err := postgres.Open(config.Postgres)
		if err != nil {
			panic(err)
		}
		cutoff := time.Now().AddDate(0, 0, -config.PrunerConfig.DaysToKeep)
		err = repository.DeleteOldJobs(db, config.PrunerConfig.BatchSize, cutoff)
		if err != nil {
			panic(err)
		}
		os.Exit(0)
	}

	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	mux := http.NewServeMux()

	startupCompleteCheck := health.NewStartupCompleteChecker()
	healthChecks := health.NewMultiChecker(startupCompleteCheck)
	health.SetupHttpMux(mux, healthChecks)

	shutdownGateway := grpc.CreateGatewayHandler(
		config.GrpcPort,
		mux,
		"/api/",
		[]string{},
		lookoutApi.SwaggerJsonTemplate(),
		lookoutApi.RegisterLookoutHandler)

	// UI config
	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		configHandler(config.UIConfig, w)
	})

	// server static UI files
	mux.Handle("/", http.FileServer(serve.CreateDirWithIndexFallback("./internal/lookout/ui/build")))

	shutdownServer := common.ServeHttp(config.HttpPort, mux)

	shutdown, wg := lookout.StartUp(config, healthChecks)
	go func() {
		<-shutdownChannel
		shutdown()
		shutdownGateway()
		shutdownServer()
	}()

	startupCompleteCheck.MarkComplete()

	wg.Wait()
}

func configHandler(config configuration.LookoutUIConfig, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(config)
	if err != nil {
		w.WriteHeader(500)
	}
}
