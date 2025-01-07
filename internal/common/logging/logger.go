package logging

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

// Logger wraps a zerolog.Logger so that the rest of the code doesn't depend directly on zerolog
type Logger struct {
	underlying zerolog.Logger
}

// FromZerolog returns a new Logger backed by the supplied zerolog.Logger
func FromZerolog(l zerolog.Logger) *Logger {
	return &Logger{
		underlying: l,
	}
}

// Debug logs a message at level Debug
func (l *Logger) Debug(args ...any) {
	l.underlying.Debug().Msg(fmt.Sprint(args...))
}

// Info logs a message at level Info
func (l *Logger) Info(args ...any) {
	l.underlying.Info().Msg(fmt.Sprint(args...))
}

// Warn logs a message at level Warn
func (l *Logger) Warn(args ...any) {
	l.underlying.Warn().Msg(fmt.Sprint(args...))
}

// Error logs a message at level Error
func (l *Logger) Error(args ...any) {
	l.underlying.Error().Msg(fmt.Sprint(args...))
}

// Panic logs a message at level Panic and panics
func (l *Logger) Panic(args ...any) {
	l.underlying.Panic().Msg(fmt.Sprint(args...))
}

// Fatal logs a message at level Fatal and exits the application
func (l *Logger) Fatal(args ...any) {
	l.underlying.Fatal().Msg(fmt.Sprint(args...))
}

// Debugf logs a formatted message at level Debug
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.underlying.Debug().Msgf(format, args...)
}

// Infof logs a formatted message at level Info
func (l *Logger) Infof(format string, args ...interface{}) {
	l.underlying.Info().Msgf(format, args...)
}

// Warnf logs a formatted message at level Warn
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.underlying.Warn().Msgf(format, args...)
}

// Errorf logs a formatted message at level Error
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.underlying.Error().Msgf(format, args...)
}

// Fatalf logs a formatted message at level Fatal and exits the application
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.underlying.Fatal().Msgf(format, args...)
}

// WithError returns a new Logger with the error added as a field
func (l *Logger) WithError(err error) *Logger {
	return &Logger{
		underlying: l.underlying.
			With().
			AnErr("error", err).
			CallerWithSkipFrameCount(StdSkipFrames - 1).
			Logger(),
	}
}

// WithStacktrace returns a new Logger with the error and (if available) the stacktrace added as fields
func (l *Logger) WithStacktrace(err error) *Logger {
	logger := l.WithError(err)
	if stackErr, ok := err.(stackTracer); ok {
		return logger.WithField("stacktrace", fmt.Sprintf("%+v", stackErr.StackTrace()))
	}
	return logger
}

// WithField returns a new Logger with the key-value pair added as a new field
func (l *Logger) WithField(key string, value any) *Logger {
	return &Logger{
		underlying: l.underlying.
			With().
			Interface(key, value).
			CallerWithSkipFrameCount(StdSkipFrames - 1).
			Logger(),
	}
}

// WithFields returns a new Logger with all key-value pairs in the map added as new fields
func (l *Logger) WithFields(args map[string]any) *Logger {
	event := l.underlying.With()
	for key, value := range args {
		event = event.Interface(key, value)
	}
	return &Logger{
		underlying: event.CallerWithSkipFrameCount(StdSkipFrames - 1).Logger(),
	}
}

// WithCallerSkip returns a new Logger with the number of callers skipped increased by the skip amount.
// This is needed when building wrappers around the Logger so as to prevent us from always reporting the
// wrapper code as the caller.
func (l *Logger) WithCallerSkip(skip int) *Logger {

	return &Logger{
		underlying: l.underlying.With().CallerWithSkipFrameCount(skip).Logger(),
	}
}

// Unexported but considered part of the stable interface of pkg/errors.
type stackTracer interface {
	StackTrace() errors.StackTrace
}
