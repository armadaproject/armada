package common

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/certs"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/weaveworks/promrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/logging"
)

const baseConfigFileName = "config"

// RFC3339Millis
const logTimestampFormat = "2006-01-02T15:04:05.999Z07:00"

func BindCommandlineArguments() {
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Error()
		os.Exit(-1)
	}
}

// TODO Move code relating to config out of common into a new package internal/serverconfig
func LoadConfig(config any, defaultPath string, overrideConfigs []string) *viper.Viper {
	v := viper.NewWithOptions(viper.KeyDelimiter("::"))
	v.SetConfigName(baseConfigFileName)
	v.AddConfigPath(defaultPath)
	if err := v.ReadInConfig(); err != nil {
		log.Errorf("Error reading base config path=%s name=%s: %v", defaultPath, baseConfigFileName, err)
		os.Exit(-1)
	}
	log.Infof("Read base config from %s", v.ConfigFileUsed())
	if len(overrideConfigs) > 0 {
		log.Infof("Read override config from %v", overrideConfigs)
	}

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

	var metadata mapstructure.Metadata
	customHooks := append(slices.Clone(commonconfig.CustomHooks), func(c *mapstructure.DecoderConfig) { c.Metadata = &metadata })
	if err := v.Unmarshal(config, customHooks...); err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	// Log a warning if there are config keys that don't match a config item in the struct the yaml is decoded into.
	// Since such unused keys indicate there's a typo in the config.
	// Also log set and unset keys at a debug level.
	if len(metadata.Keys) > 0 {
		log.Debugf("Decoded keys: %v", metadata.Keys)
	}
	if len(metadata.Unused) > 0 {
		log.Warnf("Unused keys: %v", metadata.Unused)
	}
	if len(metadata.Unset) > 0 {
		log.Debugf("Unset keys: %v", metadata.Unset)
	}

	return v
}

func UnmarshalKey(v *viper.Viper, key string, item interface{}) error {
	return v.UnmarshalKey(key, item, commonconfig.CustomHooks...)
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
	log.SetReportCaller(true)
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

	textFormatter := &log.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: logTimestampFormat,
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			fileName := path.Base(frame.File) + ":" + strconv.Itoa(frame.Line)
			return "", fileName
		},
	}

	switch strings.ToLower(formatStr) {
	case "json":
		return &log.JSONFormatter{TimestampFormat: logTimestampFormat}
	case "colourful":
		return textFormatter
	case "text":
		textFormatter.ForceColors = false
		textFormatter.DisableColors = true
		return textFormatter
	default:
		println(os.Stderr, fmt.Sprintf("Unknown log format %s, defaulting to colourful format", formatStr))
		return textFormatter
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
// TODO: Make block until a context passed in is cancelled.
func ServeHttp(port uint16, mux http.Handler) (shutdown func()) {
	return serveHttp(port, mux, false, "", "")
}

func ServeHttps(port uint16, mux http.Handler, certFile, keyFile string) (shutdown func()) {
	return serveHttp(port, mux, true, certFile, keyFile)
}

func serveHttp(port uint16, mux http.Handler, useTls bool, certFile, keyFile string) (shutdown func()) {
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	scheme := "http"
	if useTls {
		scheme = "https"
	}

	go func() {
		log.Printf("Starting %s server listening on %d", scheme, port)
		var err error
		if useTls {
			certWatcher := certs.NewCachedCertificateService(certFile, keyFile, time.Minute)
			go func() {
				certWatcher.Run(armadacontext.Background())
			}()
			srv.TLSConfig = &tls.Config{
				GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
					return certWatcher.GetCertificate(), nil
				},
			}
			err = srv.ListenAndServeTLS("", "")
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			panic(err) // TODO Don't panic, return an error
		}
	}()
	// TODO There's no need for this function to panic, since the main goroutine will exit.
	// Instead, just log an error.
	return func() {
		ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
		defer cancel()
		log.Printf("Stopping %s server listening on %d", scheme, port)
		e := srv.Shutdown(ctx)
		if e != nil {
			panic(e)
		}
	}
}
