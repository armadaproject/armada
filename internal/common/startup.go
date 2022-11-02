package common

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/weaveworks/promrus"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common/logging"
)

const baseConfigFileName = "config"

func BindCommandlineArguments() {
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Error()
		os.Exit(-1)
	}
}

// TODO Move code relating to config out of common into a new package internal/serverconfig
func LoadConfig(config interface{}, defaultPath string, overrideConfigs []string) *viper.Viper {
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigName(baseConfigFileName)
	v.AddConfigPath(defaultPath)
	if err := v.ReadInConfig(); err != nil {
		log.Errorf("Error reading base config path=%s name=%s: %v", defaultPath, baseConfigFileName, err)
		os.Exit(-1)
	}
	log.Infof("Read base config from %s", v.ConfigFileUsed())

	for _, overrideConfig := range overrideConfigs {
		v.SetConfigFile(overrideConfig)
		err := v.MergeInConfig()
		if err != nil {
			log.Errorf("Error reading config from %s: %v", overrideConfig, err)
			os.Exit(-1)
		}
		log.Infof("Read config from %s", v.ConfigFileUsed())
	}

	v.SetEnvKeyReplacer(strings.NewReplacer("::", "_"))
	v.SetEnvPrefix("ARMADA")
	v.AutomaticEnv()

	err := v.Unmarshal(config, addDecodeHook(quantityDecodeHook))
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	return v
}

func UnmarshalKey(v *viper.Viper, key string, item interface{}) error {
	return v.UnmarshalKey(key, item, addDecodeHook(quantityDecodeHook))
}

func addDecodeHook(hook mapstructure.DecodeHookFuncType) viper.DecoderConfigOption {
	return func(c *mapstructure.DecoderConfig) {
		c.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			c.DecodeHook,
			hook)
	}
}

func quantityDecodeHook(
	from reflect.Type,
	to reflect.Type,
	data interface{},
) (interface{}, error) {
	if to != reflect.TypeOf(resource.Quantity{}) {
		return data, nil
	}
	return resource.ParseQuantity(fmt.Sprintf("%v", data))
}

// TODO Move logging-related code out of common into a new package internal/logging
func ConfigureCommandLineLogging() {
	commandLineFormatter := new(logging.CommandLineFormatter)
	log.SetFormatter(commandLineFormatter)
	log.SetOutput(os.Stdout)
}

func ConfigureLogging() {
	log.SetLevel(readEnvironmentLogLevel())
	log.SetFormatter(readEnvironmentLogFormat())
	log.SetOutput(os.Stdout)
}

func readEnvironmentLogLevel() log.Level {
	level, ok := os.LookupEnv("LOG_LEVEL")
	if ok {
		logLevel, err := log.ParseLevel(level)
		if err == nil {
			return logLevel
		}
	}
	return log.InfoLevel
}

func readEnvironmentLogFormat() log.Formatter {
	formatStr, ok := os.LookupEnv("LOG_FORMAT")
	if !ok {
		formatStr = "colourful"
	}
	switch strings.ToLower(formatStr) {
	case "json":
		return &log.JSONFormatter{}
	case "colourful":
		return &log.TextFormatter{ForceColors: true, FullTimestamp: true}
	case "text":
		return &log.TextFormatter{DisableColors: true, FullTimestamp: true}
	default:
		println(os.Stderr, fmt.Sprintf("Unknown log format %s, defaulting to colourful format", formatStr))
		return &log.TextFormatter{ForceColors: true, FullTimestamp: true}
	}
}

func ServeMetrics(port uint16) (shutdown func()) {
	return ServeMetricsFor(port, prometheus.DefaultGatherer)
}

func ServeMetricsFor(port uint16, gatherer prometheus.Gatherer) (shutdown func()) {
	hook := promrus.MustNewPrometheusHook()
	log.AddHook(hook)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	return ServeHttp(port, mux)
}

// ServeHttp starts an HTTP server listening on the given port.
func ServeHttp(port uint16, mux http.Handler) (shutdown func()) {
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		log.Printf("Starting http server listening on %d", port)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			panic(err) // TODO Don't panic, return an error
		}
	}()
	// TODO There's no need for this function to panic, since the main goroutine will exit.
	// Instead, just log an error.
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Printf("Stopping http server listening on %d", port)
		e := srv.Shutdown(ctx)
		if e != nil {
			panic(e)
		}
	}
}
