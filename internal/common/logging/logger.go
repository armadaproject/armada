package logging

import (
	"fmt"
	"github.com/pkg/errors"
	"log/slog"
)

type Logger interface {
	Debug(msg string)
	Debugf(format string, args ...any)
	Info(msg string)
	Infof(format string, args ...any)
	Warn(msg string)
	Warnf(format string, args ...any)
	Error(msg string)
	Errorf(format string, args ...any)
	Fatal(msg string)
	Fatalf(format string, args ...any)
	With(key, value any) Logger
	WithError(err error) Logger
	WithStacktrace(err error) Logger
}

// Unexported but considered part of the stable interface of pkg/errors.
type stackTracer interface {
	StackTrace() errors.StackTrace
}

func NewLogger() Logger {
	return &slogLogger{
		delegate: slog.Default(),
	}
}

type slogLogger struct {
	delegate *slog.Logger
}

func (l *slogLogger) Debug(msg string) {
	l.delegate.Debug(msg)
}

func (l *slogLogger) Debugf(format string, args ...any) {
	l.delegate.Debug(fmt.Sprintf(format, args))
}

func (l *slogLogger) Infof(format string, args ...any) {
	l.delegate.Info(fmt.Sprintf(format, args))
}

func (l *slogLogger) Info(msg string) {
	l.delegate.Info(msg)
}

func (l *slogLogger) Warn(msg string) {
	l.delegate.Warn(msg)
}

func (l *slogLogger) Warnf(format string, args ...any) {
	l.delegate.Warn(fmt.Sprintf(format, args))
}

func (l *slogLogger) Error(msg string) {
	l.delegate.Error(msg)
}

func (l *slogLogger) Errorf(format string, args ...any) {
	l.delegate.Error(fmt.Sprintf(format, args))
}

func (l *slogLogger) Fatal(msg string) {
	l.delegate.Error(msg)
}

func (l *slogLogger) Fatalf(format string, args ...any) {
	l.delegate.Error(fmt.Sprintf(format, args))
}

func (l *slogLogger) With(key, value any) Logger {
	return &slogLogger{l.delegate.With(key, value)}
}

func (l *slogLogger) WithError(err error) Logger {
	return &slogLogger{l.delegate.With("error", err.Error())}
}

// WithStacktrace returns a new Logger obtained by adding error information and, if available, a stack trace
// as fields to the provided Logger
func (l *slogLogger) WithStacktrace(err error) Logger {
	logger := l.delegate.With(slog.Any("error", err.Error()))
	if stackErr, ok := err.(stackTracer); ok {
		logger = logger.With("stacktrace", fmt.Sprintf("%s", stackErr.StackTrace()))
	}
	return &slogLogger{logger}
}
