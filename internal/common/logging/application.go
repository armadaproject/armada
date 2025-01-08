package logging

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
	"sigs.k8s.io/yaml"
	"strconv"
	"strings"
)

var (
	defaultLogConfigPath = "config/logging.yaml"
	logConfigPathEnvVar  = "ARMADA_LOG_CONFIG"
	RFC3339Milli         = "2006-01-02T15:04:05.000Z07:00"
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
	configPath := getEnv(logConfigPathEnvVar, defaultLogConfigPath)
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

	if strings.ToLower(logConfig.File.Format) == "text" || strings.ToLower(logConfig.File.Format) == "colorful" {
		return createConsoleWriter(lumberjackLogger, level, logConfig.File.Format), nil
	} else {
		return createJsonWriter(lumberjackLogger, level), nil
	}
}

func createConsoleLogger(logConfig Config) (*FilteredLevelWriter, error) {
	level, err := zerolog.ParseLevel(logConfig.Console.Level)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(logConfig.Console.Format) == "text" || strings.ToLower(logConfig.Console.Format) == "colorful" {
		return createConsoleWriter(os.Stdout, level, logConfig.Console.Format), nil
	} else {
		return createJsonWriter(os.Stdout, level), nil
	}
}

func createJsonWriter(out io.Writer, level zerolog.Level) *FilteredLevelWriter {
	return &FilteredLevelWriter{
		level:  level,
		writer: out,
	}
}

func createConsoleWriter(out io.Writer, level zerolog.Level, format string) *FilteredLevelWriter {
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
			NoColor: strings.ToLower(format) == "text",
		},
	}
}

func readConfig(configFilePath string) (Config, error) {
	yamlConfig, err := os.ReadFile(configFilePath)
	if err != nil {
		return Config{}, errors.Wrap(err, "failed to read log config file")
	}

	var config Config
	err = yaml.Unmarshal(yamlConfig, &config)
	if err != nil {
		return Config{}, errors.Wrap(err, "failed to unmarshall log config file")
	}
	err = validate(config)
	if err != nil {
		return Config{}, errors.Wrap(err, "invalid log configuration")
	}
	return config, nil
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

type FilteredLevelWriter struct {
	writer io.Writer
	level  zerolog.Level
}

// Write writes to the underlying Writer.
func (w *FilteredLevelWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

// WriteLevel calls WriteLevel of the underlying Writer only if the level is equal
// or above the Level.
func (w *FilteredLevelWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	if level >= w.level {
		return w.writer.Write(p)
	}
	return len(p), nil
}
