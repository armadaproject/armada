package logging

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Logger wraps a *zap.SugaredLogger so that the rest of the code doesn't depend directly on Zap
type Logger struct {
	undlerlying *zap.SugaredLogger
}

// FromZap returns a New Logger backed by the supplied *zap.SugaredLogger
func FromZap(l *zap.Logger) *Logger {
	return &Logger{
		undlerlying: l.Sugar(),
	}
}

// Debug logs a message at level Debug
func (l *Logger) Debug(args ...any) {
	l.undlerlying.Debug(args...)
}

// Info logs a message at level Info
func (l *Logger) Info(args ...any) {
	l.undlerlying.Info(args...)
}

// Warn logs a message at level Warn
func (l *Logger) Warn(args ...any) {
	l.undlerlying.Warn(args...)
}

// Error logs a message at level Error
func (l *Logger) Error(args ...any) {
	l.undlerlying.Error(args...)
}

// Panic logs a message at level Panic
func (l *Logger) Panic(args ...any) {
	l.undlerlying.Panic(args...)
}

// Fatal logs a message at level Fatal then the process will exit with status set to 1.
func (l *Logger) Fatal(args ...any) {
	l.undlerlying.Fatal(args...)
}

// Debugf logs a message at level Debug.
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.undlerlying.Debugf(format, args...)
}

// Infof logs a message at level Info.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.undlerlying.Infof(format, args...)
}

// Warnf logs a message at level Warn.
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.undlerlying.Warnf(format, args...)
}

// Errorf logs a message at level Error.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.undlerlying.Errorf(format, args...)
}

// Fatalf logs a message at level Fatal.
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.undlerlying.Fatalf(format, args...)
}

// WithError returns a new Logger with the error added as a field
func (l *Logger) WithError(err error) *Logger {
	return l.WithField("error", err)
}

// WithStacktrace returns a new Logger with the error and (if available) the stacktrace added as fields
func (l *Logger) WithStacktrace(err error) *Logger {
	logger := l.WithError(err)
	if stackErr, ok := err.(stackTracer); ok {
		return logger.WithField("stacktrace", stackErr.StackTrace())
	} else {
		return logger
	}
}

// WithField returns a new Logger with the key-value pair added as a new field
func (l *Logger) WithField(key string, value any) *Logger {
	return &Logger{
		undlerlying: l.undlerlying.With(key, value),
	}
}

// WithFields returns a new Logger with all key-value pairs in the map added as new fields
func (l *Logger) WithFields(args map[string]any) *Logger {
	fields := make([]any, 0, len(args))
	for key, value := range args {
		fields = append(fields, zap.Any(key, value))
	}
	return &Logger{
		undlerlying: l.undlerlying.With(fields...),
	}
}

// WithCallerSkip returns a new Logger with the number of callers skipped increased by the skip amount.
// This is needed when building wrappers around the Logger so as to prevent us from always reporting the
// wrapper code as the caller.
func (l *Logger) WithCallerSkip(skip int) *Logger {
	return &Logger{
		undlerlying: l.undlerlying.WithOptions(zap.AddCallerSkip(skip)),
	}
}

// Unexported but considered part of the stable interface of pkg/errors.
type stackTracer interface {
	StackTrace() errors.StackTrace
}
