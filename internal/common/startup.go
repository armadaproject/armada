package common

import (
	"context"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/weaveworks/promrus"
	"net/http"
	"os"
	"strings"
	"time"
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

func ConfigureLogging() {
	log.SetFormatter(&log.TextFormatter{ForceColors: true, FullTimestamp: true})
	log.SetOutput(os.Stdout)
}

func ServeMetrics(addr string) (shutdown func()) {
	srv := &http.Server{Addr: addr}

	hook := promrus.MustNewPrometheusHook()
	log.AddHook(hook)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Printf("Metrics listening on %s", addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			panic(err)
		}
		log.Printf("Metrics listening on %s", addr)
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Print("Stopping metrics server")
		e := srv.Shutdown(ctx)
		if e != nil {
			panic(e)
		}
	}
}
