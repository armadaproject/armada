package logging

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"
)

type LogFormat string

// Allowed values for LogFormat.
const (
	FormatText      LogFormat = "text"
	FormatJSON      LogFormat = "json"
	FormatColourful LogFormat = "colourful"
)

// Config defines Armada logging configuration.
type Config struct {
	// Defines configuration for console logging on stdout
	Console struct {
		// Log level, e.g. INFO, ERROR etc
		Level string `yaml:"level"`
		// Logging format, either text, json or colourful
		Format LogFormat `yaml:"format"`
	} `yaml:"console"`
	// Defines configuration for file logging
	File struct {
		// Whether file logging is enabled.
		Enabled bool `yaml:"enabled"`
		// Log level, e.g. INFO, ERROR etc
		Level string `yaml:"level"`
		// Logging format, either text, json or or colourful
		Format LogFormat `yaml:"format"`
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
	_, err := zerolog.ParseLevel(c.Console.Level)
	if err != nil {
		return err
	}

	if c.File.Enabled {
		_, err := zerolog.ParseLevel(c.File.Level)
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

func (lf *LogFormat) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}

	switch LogFormat(s) {
	case FormatText, FormatJSON, FormatColourful:
		*lf = LogFormat(s)
		return nil
	default:
		return fmt.Errorf("invalid log format %q: valid values are %q, %q, or %q", s, FormatText, FormatJSON, FormatColourful)
	}
}
