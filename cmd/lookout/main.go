package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/grpc"
	"github.com/armadaproject/armada/internal/common/health"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/profiling"
	"github.com/armadaproject/armada/internal/common/serve"
	"github.com/armadaproject/armada/internal/lookout"
	"github.com/armadaproject/armada/internal/lookout/configuration"
	"github.com/armadaproject/armada/internal/lookout/postgres"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookout/repository/schema"
	lookoutApi "github.com/armadaproject/armada/pkg/api/lookout"
)

const (
	CustomConfigLocation string = "config"
	MigrateDatabase      string = "migrateDatabase"
	PruneDatabase               = "pruneDatabase"
)

func init() {
	pflag.StringSlice(
		CustomConfigLocation,
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)",
	)
	pflag.Bool(MigrateDatabase, false, "Migrate database instead of running server")
	pflag.Bool(PruneDatabase, false, "Removes old jobs from the database instead of running server")
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

	if viper.GetBool(PruneDatabase) {
		db, err := postgres.Open(config.Postgres)
		if err != nil {
			panic(err)
		}
		if config.PrunerConfig.DaysToKeep <= 0 {
			panic(fmt.Errorf("invalid PrunerConfig.DaysToKeep [%v]: must be greater than 0", config.PrunerConfig.DaysToKeep))
		}
		cutoff := time.Now().AddDate(0, 0, -config.PrunerConfig.DaysToKeep)
		batchSize := config.PrunerConfig.BatchSize
		if batchSize <= 0 {
			panic(fmt.Errorf("invalid PrunerConfig.BatchSize [%v]: must be greater than 0", batchSize))
		}
		err = repository.DeleteOldJobs(db, batchSize, cutoff)
		if err != nil {
			panic(err)
		}
		os.Exit(0)
	}

	// Expose profiling endpoints if enabled.
	pprofServer := profiling.SetupPprofHttpServer(config.PprofPort)
	go func() {
		ctx := armadacontext.Background()
		if err := serve.ListenAndServe(ctx, pprofServer); err != nil {
			logging.WithStacktrace(ctx, err).Error("pprof server failure")
		}
	}()

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
		false,
		config.Grpc.Tls.Enabled,
		[]string{},
		lookoutApi.SwaggerJsonTemplate(),
		lookoutApi.RegisterLookoutHandler)

	// UI config
	mux.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		configHandler(config.UIConfig, w)
	})

	// server static UI files
	mux.Handle("/", http.FileServer(serve.CreateDirWithIndexFallback("./internal/lookout/ui/build")))

	var shutdownServer func() = nil
	if config.Grpc.Tls.Enabled {
		shutdownServer = common.ServeHttps(config.HttpPort, mux, config.Grpc.Tls.CertPath, config.Grpc.Tls.KeyPath)
	} else {
		shutdownServer = common.ServeHttp(config.HttpPort, mux)
	}

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
