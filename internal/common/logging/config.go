package logging

import (
	"golang.org/x/exp/maps"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
)

var validLogFormats = map[string]bool{
	"text": true,
	"json": true,
}

// Config defines Armada logging configuration.
type Config struct {
	// Defines configuration for console logging on stdout
	Console struct {
		// Log level, e.g. INFO, ERROR etc
		Level string `yaml:"level"`
		// Logging format, either text or json
		Format string `yaml:"format"`
	} `yaml:"console"`
	// Defines configuration for file logging
	File struct {
		// Whether file logging is enabled.
		Enabled bool `yaml:"enabled"`
		// Log level, e.g. INFO, ERROR etc
		Level string `yaml:"level"`
		// Logging format, either text or json
		Format string `yaml:"format"`
		// The Location of the logfile on disk
		LogFile string `yaml:"logfile"`
		// Log Rotation Options
		Rotation struct {
			// Whether Log Rotation is enabled
			Enabled bool `yaml:"enabled"`
			// Maximum size in megabytes of the log file before it gets rotated
			MaxSizeMb int `yaml:"maxSizeMb"`
			// Maximum number of old log files to retain
			MaxBackups int `yaml:"maxBackups"`
			// Maximum number of days to retain old log files
			MaxAgeDays int `yaml:"maxAgeDays"`
			// Whether to compress rotated log files
			Compress bool `yaml:"compress"`
		} `yaml:"rotation"`
	} `yaml:"file"`
}

func validate(c Config) error {
	_, err := parseLogLevel(c.Console.Level)
	if err != nil {
		return err
	}

	err = validateLogFormat(c.Console.Format)
	if err != nil {
		return err
	}

	if c.File.Enabled {
		_, err := parseLogLevel(c.File.Level)
		if err != nil {
			return err
		}

		err = validateLogFormat(c.File.Format)
		if err != nil {
			return err
		}

		rotation := c.File.Rotation
		if rotation.Enabled {
			if rotation.MaxSizeMb <= 0 {
				return errors.New("rotation.maxSizeMb must be greater than zero")
			}
			if rotation.MaxBackups <= 0 {
				return errors.New("rotation.maxBackups must be greater than zero")
			}
			if rotation.MaxAgeDays <= 0 {
				return errors.New("rotation.maxAgeDays must be greater than zero")
			}
		}
	}

	return nil
}

func validateLogFormat(f string) error {
	_, ok := validLogFormats[f]
	if !ok {
		err := errors.Errorf("unknown log format: %s.  Valid formats are %s", f, maps.Keys(validLogFormats))
		return err
	}
	return nil
}

func parseLogLevel(level string) (zapcore.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "panic":
		return zapcore.PanicLevel, nil
	case "fatal":
		return zapcore.FatalLevel, nil
	default:
		return zapcore.InfoLevel, errors.Errorf("unknown level: %s", level)
	}
}
