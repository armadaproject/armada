package logging

import (
	"fmt"
	"os"

	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"sigs.k8s.io/yaml"
)

var (
	defaultLogConfigPath = "config/logging.yaml"
	logConfigPathEnvVar  = "ARMADA_LOG_CONFIG"
)

// MustConfigureApplicationLogging sets up logging suitable for an application.  Logging configuration is loaded from
// a filepath given by the ARMADA_LOG_CONFIG environmental variable or from config/logging.yaml if this var is unset.
// Note that this function will immediately shut do the application if it fails.
func MustConfigureApplicationLogging() {
	err := ConfigureApplicationLogging()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error initialising logging"+err.Error())
		os.Exit(1)
	}
}

// ConfigureApplicationLogging sets up logging suitable for an application.  Logging configuration is loaded from
// a filepath given by the ARMADA_LOG_CONFIG environmental variable or from config/logging.yaml if this var is unset.
func ConfigureApplicationLogging() error {
	configPath := getEnv(logConfigPathEnvVar, defaultLogConfigPath)

	logConfig, err := readConfig(configPath)
	if err != nil {
		return err
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:          "ts",
		LevelKey:         "level",
		NameKey:          "logger",
		CallerKey:        "caller",
		FunctionKey:      zapcore.OmitKey,
		MessageKey:       "msg",
		StacktraceKey:    "stacktrace",
		LineEnding:       zapcore.DefaultLineEnding,
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		EncodeTime:       zapcore.ISO8601TimeEncoder,
		EncodeDuration:   zapcore.SecondsDurationEncoder,
		EncodeCaller:     shortCallerEncoder,
		ConsoleSeparator: " ",
	}

	var cores []zapcore.Core

	// Console logging
	consoleEncoder, err := createEncoder(logConfig.Console.Format, encoderConfig)
	if err != nil {
		return errors.Wrap(err, "error creating console logger")
	}
	consoleLevel, err := parseLogLevel(logConfig.Console.Level)
	if err != nil {
		return errors.Wrap(err, "error creating console logger")
	}
	cores = append(cores, zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), consoleLevel))

	// File logging
	if logConfig.File.Enabled {
		fileEncoder, err := createEncoder(logConfig.File.Format, encoderConfig)
		if err != nil {
			return errors.Wrap(err, "error creating file logger")
		}
		w := zapcore.AddSync(&lumberjack.Logger{
			Filename:   "app.log",
			MaxSize:    logConfig.File.Rotation.MaxSizeMb,
			MaxBackups: logConfig.File.Rotation.MaxBackups,
			MaxAge:     logConfig.File.Rotation.MaxAgeDays, // days
			Compress:   logConfig.File.Rotation.Compress,
		})
		fileLevel, err := parseLogLevel(logConfig.Console.Level)
		if err != nil {
			return errors.Wrap(err, "error creating file logger")
		}
		cores = append(cores, zapcore.NewCore(fileEncoder, w, fileLevel))
	}

	core := zapcore.NewTee(cores...)

	// Create logger
	l := zap.New(core, zap.AddCaller()).WithOptions(zap.AddCallerSkip(2))
	ReplaceStdLogger(FromZap(l))

	return nil
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

func createEncoder(format string, encoderConfig zapcore.EncoderConfig) (zapcore.Encoder, error) {
	switch strings.ToLower(format) {
	case "json":
		return zapcore.NewJSONEncoder(encoderConfig), nil
	case "text":
		return zapcore.NewConsoleEncoder(encoderConfig), nil
	default:
		return nil, errors.Errorf("unknown format: %s", format)
	}
}

// ShortCallerEncoder serializes a caller in to just file:line format.
func shortCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	trimmed := caller.TrimmedPath()
	lastSlash := strings.LastIndexByte(trimmed, '/')
	if lastSlash != -1 && lastSlash != len(trimmed)-1 {
		fileName := trimmed[lastSlash+1:]
		enc.AppendString(fileName)
	} else {
		enc.AppendString(trimmed)
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
