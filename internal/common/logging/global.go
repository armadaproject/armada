package logging

import (
	"os"

	"github.com/rs/zerolog"
)

var (
	// The global Logger.  Comes configured with some sensible defaults for e.g. unit tests, but applications should
	// generally configure their own logging config via ReplaceStdLogger
	stdLogger = createDefaultLogger()
	// stdLogger with one extra caller frame skipped. Used by the package-level log functions below, which
	// sit between the logging site and the Logger.
	loggerWithSkip = stdLogger.WithCallerSkip(1)
)

// ReplaceStdLogger Replaces the global logger.  This should be called once at app startup!
func ReplaceStdLogger(l *Logger) {
	stdLogger = l
	loggerWithSkip = l.WithCallerSkip(1)
}

// StdLogger Returns the default logger
func StdLogger() *Logger {
	return stdLogger
}

// Debug logs a message at level Debug on the default logger.
func Debug(args ...any) {
	loggerWithSkip.Debug(args...)
}

// Info logs a message at level Info on the default logger.
func Info(args ...any) {
	loggerWithSkip.Info(args...)
}

// Warn logs a message at level Warn on the default logger.
func Warn(args ...any) {
	loggerWithSkip.Warn(args...)
}

// Error logs a message at level Error on the default logger.
func Error(args ...any) {
	loggerWithSkip.Error(args...)
}

// Panic logs a message at level Panic on the default logger.
func Panic(args ...any) {
	loggerWithSkip.Panic(args...)
}

// Fatal logs a message at level Fatal on the default logger then the process will exit with status set to 1.
func Fatal(args ...any) {
	loggerWithSkip.Fatal(args...)
}

// Debugf logs a message at level Debug on the default logger.
func Debugf(format string, args ...any) {
	loggerWithSkip.Debugf(format, args...)
}

// Infof logs a message at level Info on the default logger.
func Infof(format string, args ...any) {
	loggerWithSkip.Infof(format, args...)
}

// Warnf logs a message at level Warn on the default logger.
func Warnf(format string, args ...any) {
	loggerWithSkip.Warnf(format, args...)
}

// Errorf logs a message at level Error on the default logger.
func Errorf(format string, args ...any) {
	loggerWithSkip.Errorf(format, args...)
}

// Fatalf logs a message at level Fatal on the default logger then the process will exit with status set to 1.
func Fatalf(format string, args ...any) {
	loggerWithSkip.Fatalf(format, args...)
}

// WithField returns a new Logger with the key-value pair added as a new field
func WithField(key string, value any) *Logger {
	return stdLogger.WithField(key, value)
}

// WithFields returns a new Logger with all key-value pairs in the map added as new fields
func WithFields(args map[string]any) *Logger {
	return stdLogger.WithFields(args)
}

// WithError returns a new Logger with the error added as a field
func WithError(err error) *Logger {
	return stdLogger.WithError(err)
}

// WithStacktrace returns a new Logger with the error and (if available) the stacktrace added as fields
func WithStacktrace(err error) *Logger {
	return stdLogger.WithStacktrace(err)
}

// createDefaultLogger returns a new Logger configured with default settings using zerolog.
func createDefaultLogger() *Logger {
	zerolog.TimeFieldFormat = RFC3339Milli // needs to be higher or greater precision than the writer format.
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: RFC3339Milli,
		NoColor:    true,
	}
	zerologLogger := zerolog.New(consoleWriter).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Logger()

	return FromZerolog(zerologLogger)
}
