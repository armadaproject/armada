package main

import (
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/grpc"
	"github.com/G-Research/armada/internal/common/serve"
	"github.com/G-Research/armada/internal/lookout"
	"github.com/G-Research/armada/internal/lookout/configuration"
	"github.com/G-Research/armada/internal/lookout/postgres"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	lookoutApi "github.com/G-Research/armada/pkg/api/lookout"
)

const CustomConfigLocation string = "config"
const MigrateDatabase string = "migrateDatabase"

func init() {
	pflag.String(CustomConfigLocation, "", "Fully qualified path to application configuration file")
	pflag.Bool(MigrateDatabase, false, "Migrate database instead of running server")
	pflag.Parse()
}

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.LookoutConfiguration
	userSpecifiedConfig := viper.GetString(CustomConfigLocation)
	common.LoadConfig(&config, "./config/lookout", userSpecifiedConfig)

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

	shutdownChannel := make(chan os.Signal, 1)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)

	shutdownMetricServer := common.ServeMetrics(config.MetricsPort)
	defer shutdownMetricServer()

	mux, shutdownGateway := grpc.CreateGatewayHandler(
		config.GrpcPort,
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

	shutdown, wg := lookout.StartUp(config)
	go func() {
		<-shutdownChannel
		shutdown()
		shutdownGateway()
		shutdownServer()
	}()
	wg.Wait()
}

func configHandler(config configuration.LookoutUIConfig, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(config)
	if err != nil {
		w.WriteHeader(500)
	}
}
