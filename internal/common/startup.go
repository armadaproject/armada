package common

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/weaveworks/promrus"

	"github.com/G-Research/armada/internal/common/logging"
)

func BindCommandlineArguments() {
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Error()
		os.Exit(-1)
	}
}

func LoadConfig(config interface{}, defaultPath string, overrideConfig string) {
	viper.SetConfigName("config")
	viper.AddConfigPath(defaultPath)
	if err := viper.ReadInConfig(); err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	if overrideConfig != "" {
		viper.SetConfigFile(overrideConfig)

		err := viper.MergeInConfig()
		if err != nil {
			log.Error(err)
			os.Exit(-1)
		}
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("ARMADA")
	viper.AutomaticEnv()

	err := viper.Unmarshal(config)
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}
}
func ConfigureCommandLineLogging() {
	commandLineFormatter := new(logging.CommandLineFormatter)
	log.SetFormatter(commandLineFormatter)
	log.SetOutput(os.Stdout)
}

func ConfigureLogging() {
	log.SetFormatter(&log.TextFormatter{ForceColors: true, FullTimestamp: true})
	log.SetOutput(os.Stdout)
}

func ServeMetrics(port uint16) (shutdown func()) {
	hook := promrus.MustNewPrometheusHook()
	log.AddHook(hook)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return ServeHttp(port, mux)
}

func ServeHttp(port uint16, mux http.Handler) (shutdown func()) {
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux}

	go func() {
		log.Printf("Starting server listening on %d", port)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Printf("Stopping server listening on %d", port)
		e := srv.Shutdown(ctx)
		if e != nil {
			panic(e)
		}
	}
}
