package logging

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// The global Logger.  Comes configured with some sensible defaults for e.g. unit tests, but applications should
// generally configure their own logging config via ReplaceStdLogger
var stdLogger = &Logger{underlying: createDefaultLogger()}

// ReplaceStdLogger Replaces the global logger.  This should be called once at app startup!
func ReplaceStdLogger(l *Logger) {
	stdLogger = l
}

// StdLogger Returns the default logger
func StdLogger() *Logger {
	return stdLogger
}

// Debug logs a message at level Debug.
func Debug(args ...any) {
	stdLogger.Debug(args...)
}

// Info logs a message at level Info.
func Info(args ...any) {
	stdLogger.Info(args...)
}

// Warn logs a message at level Warn on the standard logger.
func Warn(args ...any) {
	stdLogger.Warn(args...)
}

// Error logs a message at level Error on the standard logger.
func Error(args ...any) {
	stdLogger.Error(args...)
}

// Panic logs a message at level Panic on the standard logger.
func Panic(args ...any) {
	stdLogger.Panic(args...)
}

// Fatal logs a message at level Fatal on the standard logger then the process will exit with status set to 1.
func Fatal(args ...any) {
	stdLogger.Fatal(args...)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(format string, args ...any) {
	stdLogger.Debugf(format, args...)
}

// Infof logs a message at level Info on the standard logger.
func Infof(format string, args ...any) {
	stdLogger.Infof(format, args...)
}

// Warnf logs a message at level Warn on the standard logger.
func Warnf(format string, args ...any) {
	stdLogger.Warnf(format, args...)
}

// Errorf logs a message at level Error on the standard logger.
func Errorf(format string, args ...any) {
	stdLogger.Errorf(format, args...)
}

// Fatalf logs a message at level Fatal on the standard logger then the process will exit with status set to 1.
func Fatalf(format string, args ...any) {
	stdLogger.Fatalf(format, args...)
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

// Default logging options
func createDefaultLogger() *zap.SugaredLogger {
	pe := zap.NewProductionEncoderConfig()
	pe.EncodeTime = zapcore.ISO8601TimeEncoder
	pe.ConsoleSeparator = " "
	pe.EncodeLevel = zapcore.CapitalLevelEncoder
	consoleEncoder := zapcore.NewConsoleEncoder(pe)
	core := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zapcore.DebugLevel)
	return zap.
		New(core, zap.AddCaller()).
		WithOptions(zap.AddCallerSkip(2)).
		Sugar()
}
