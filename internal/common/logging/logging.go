package logging

import (
	"go.uber.org/zap"
)

// Debug logs a message at level Debug on the standard logger.
func Debug(msg string) {
	zap.S().Debug(msg)
}

// Info logs a message at level Info on the standard logger.
func Info(msg string) {
	zap.S().Info(msg)
}

// Warn logs a message at level Warn on the standard logger.
func Warn(msg string) {
	zap.S().Warn(msg)
}

// Error logs a message at level Error on the standard logger.
func Error(msg string) {
	zap.S().Error(msg)
}

// Panic logs a message at level Panic on the standard logger.
func Panic(msg string) {
	zap.S().Panic(msg)
}

// Fatal logs a message at level Fatal on the standard logger then the process will exit with status set to 1.
func Fatal(msg string) {
	zap.S().Fatal(msg)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(format string, args ...interface{}) {
	zap.S().Debugf(format, args...)
}

// Infof logs a message at level Info on the standard logger.
func Infof(format string, args ...interface{}) {
	zap.S().Infof(format, args...)
}

// Warnf logs a message at level Warn on the standard logger.
func Warnf(format string, args ...interface{}) {
	zap.S().Warnf(format, args...)
}

// Errorf logs a message at level Error on the standard logger.
func Errorf(format string, args ...interface{}) {
	zap.S().Errorf(format, args...)
}

// Panicf logs a message at level Panic on the standard logger.
func Panicf(format string, args ...interface{}) {
	zap.S().Panicf(format, args...)
}

// Fatalf logs a message at level Fatal on the standard logger then the process will exit with status set to 1.
func Fatalf(format string, args ...interface{}) {
	zap.S().Fatalf(format, args...)
}

func WithError(err error) *zap.SugaredLogger {
	return zap.S().With("error", err)
}
