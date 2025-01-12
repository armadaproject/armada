package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultConfigPath = "config/logging.yaml"
	configPathEnvVar  = "ARMADA_LOG_CONFIG"
	RFC3339Milli      = "2006-01-02T15:04:05.000Z07:00"

	// legacy env vars: will be removed in a future release
	legacyLogFormatEnvVar = "LOG_FORMAT"
	legacyLogLevelEnvVar  = "LOG_LEVEL"
)

// MustConfigureApplicationLogging sets up logging suitable for an application. Logging configuration is loaded from
// a filepath given by the ARMADA_LOG_CONFIG environmental variable or from config/logging.yaml if this var is unset.
// Note that this function will immediately shut down the application if it fails.
func MustConfigureApplicationLogging() {
	err := ConfigureApplicationLogging()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error initializing logging: "+err.Error())
		os.Exit(1)
	}
}

// ConfigureApplicationLogging sets up logging suitable for an application. Logging configuration is loaded from
// a filepath given by the ARMADA_LOG_CONFIG environmental variable or from config/logging.yaml if this var is unset.
func ConfigureApplicationLogging() error {
	// Set some global logging properties
	zerolog.TimeFieldFormat = RFC3339Milli // needs to be higher or greater precision than the writer format.
	zerolog.CallerMarshalFunc = shortCallerEncoder

	// Load config file
	configPath := getEnv(configPathEnvVar, defaultConfigPath)
	logConfig, err := readConfig(configPath)
	if err != nil {
		return err
	}

	// Console logging
	var writers []io.Writer
	consoleLogger, err := createConsoleLogger(logConfig)
	if err != nil {
		return err
	}
	writers = append(writers, consoleLogger)

	// File logging
	if logConfig.File.Enabled {
		fileLogger, err := createFileLogger(logConfig)
		if err != nil {
			return err
		}
		writers = append(writers, fileLogger)
	}

	// Combine loggers
	multiWriter := zerolog.MultiLevelWriter(writers...)
	logger := zerolog.New(multiWriter).With().Timestamp().Logger()

	// Set our new logger to be the default
	ReplaceStdLogger(FromZerolog(logger))
	return nil
}

func createFileLogger(logConfig Config) (*FilteredLevelWriter, error) {
	level, err := zerolog.ParseLevel(logConfig.File.Level)
	if err != nil {
		return nil, err
	}

	// Set up lumberjack for log rotation
	lumberjackLogger := &lumberjack.Logger{
		Filename:   logConfig.File.LogFile,
		MaxSize:    logConfig.File.Rotation.MaxSizeMb,
		MaxBackups: logConfig.File.Rotation.MaxBackups,
		MaxAge:     logConfig.File.Rotation.MaxAgeDays,
		Compress:   logConfig.File.Rotation.Compress,
	}

	return createWriter(lumberjackLogger, level, logConfig.File.Format)
}

func createConsoleLogger(logConfig Config) (*FilteredLevelWriter, error) {
	level, err := zerolog.ParseLevel(logConfig.Console.Level)
	if err != nil {
		return nil, err
	}
	return createWriter(os.Stdout, level, logConfig.Console.Format)
}

func createWriter(out io.Writer, level zerolog.Level, format LogFormat) (*FilteredLevelWriter, error) {
	level = overrideLevelFromEnv(level)
	format = overrideFormatFromEnv(format)

	switch format {
	case FormatJSON:
		return &FilteredLevelWriter{
			level:  level,
			writer: out,
		}, nil
	case FormatColourful, FormatText:
		return &FilteredLevelWriter{
			level: level,
			writer: zerolog.ConsoleWriter{
				Out:        out,
				TimeFormat: RFC3339Milli,
				FormatLevel: func(i interface{}) string {
					return strings.ToUpper(fmt.Sprintf("%s", i))
				},
				FormatCaller: func(i interface{}) string {
					return filepath.Base(fmt.Sprintf("%s", i))
				},
				NoColor: format == FormatText,
			},
		}, nil
	}
	return nil, errors.Errorf("unknown log format: %s", format)
}

func shortCallerEncoder(_ uintptr, file string, line int) string {
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = short
	return file + ":" + strconv.Itoa(line)
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func overrideLevelFromEnv(level zerolog.Level) zerolog.Level {
	levelOverrideStr, ok := os.LookupEnv(legacyLogLevelEnvVar)
	if !ok {
		return level
	}
	if ok {
		levelOverride, err := zerolog.ParseLevel(levelOverrideStr)
		if err == nil {
			return levelOverride
		}
	}
	return level
}

func overrideFormatFromEnv(format LogFormat) LogFormat {
	levelFormatStr, ok := os.LookupEnv(legacyLogFormatEnvVar)
	if !ok {
		return format
	}
	switch strings.ToLower(levelFormatStr) {
	case "json":
		return FormatJSON
	case "colourful":
		return FormatColourful
	case "text":
		return FormatText
	}
	return format
}
