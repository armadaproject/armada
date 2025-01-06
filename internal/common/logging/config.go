package logging

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
