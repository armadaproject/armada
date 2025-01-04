package logging

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Unexported but considered part of the stable interface of pkg/errors.
type stackTracer interface {
	StackTrace() errors.StackTrace
}

type Logger struct {
	undlerlying *zap.SugaredLogger
}

func FromZap(l *zap.Logger) *Logger {
	return &Logger{
		undlerlying: l.Sugar(),
	}
}

// Debug logs a message at level Debug
func (l *Logger) Debug(args ...any) {
	l.undlerlying.Debug(args)
}

// Info logs a message at level Info
func (l *Logger) Info(args ...any) {
	l.undlerlying.Info(args)
}

// Warn logs a message at level Warn
func (l *Logger) Warn(args ...any) {
	l.undlerlying.Warn(args)
}

// Error logs a message at level Error
func (l *Logger) Error(args ...any) {
	l.undlerlying.Error(args)
}

// Panic logs a message at level Panic
func (l *Logger) Panic(args ...any) {
	l.undlerlying.Panic(args)
}

// Fatal logs a message at level Fatal then the process will exit with status set to 1.
func (l *Logger) Fatal(args string) {
	l.undlerlying.Fatal(args)
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

func (l *Logger) WithError(err error) *Logger {
	return l.WithField("error", err.Error())
}

// WithStacktrace returns a new Logger obtained by adding error information and, if available, a stack trace
// as fields
func (l *Logger) WithStacktrace(err error) *Logger {
	logger := l.WithError(err)
	if stackErr, ok := err.(stackTracer); ok {
		return logger.WithField("stacktrace", stackErr.StackTrace())
	} else {
		return logger
	}
}

func (l *Logger) WithField(args ...any) *Logger {
	return &Logger{
		undlerlying: l.undlerlying.With(args...),
	}
}

func (l *Logger) WithFields(args map[string]any) *Logger {
	fields := make([]any, 0, len(args))
	for key, value := range args {
		fields = append(fields, zap.Any(key, value))
	}
	return With(fields...)
}

func (l *Logger) WithCallerSkip(skip int) *Logger {
	return &Logger{
		undlerlying: l.undlerlying.WithOptions(zap.AddCallerSkip(skip)),
	}
}

var stdLogger = &Logger{zap.S()}

func StdLogger() *Logger {
	return stdLogger
}

func SetDefaultLogger(l *Logger) {
	stdLogger = l
}

// Debug logs a message at level Debug.
func Debug(msg string) {
	stdLogger.Debug(msg)
}

// Info logs a message at level Info.
func Info(msg string) {
	stdLogger.Info(msg)
}

// Warn logs a message at level Warn on the standard logger.
func Warn(msg string) {
	stdLogger.Warn(msg)
}

// Error logs a message at level Error on the standard logger.
func Error(msg string) {
	stdLogger.Error(msg)
}

// Panic logs a message at level Panic on the standard logger.
func Panic(msg string) {
	stdLogger.Panic(msg)
}

// Fatal logs a message at level Fatal on the standard logger then the process will exit with status set to 1.
func Fatal(msg string) {
	stdLogger.Fatal(msg)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(format string, args ...interface{}) {
	stdLogger.Debugf(format, args...)
}

// Infof logs a message at level Info on the standard logger.
func Infof(format string, args ...interface{}) {
	stdLogger.Infof(format, args...)
}

// Warnf logs a message at level Warn on the standard logger.
func Warnf(format string, args ...interface{}) {
	stdLogger.Warnf(format, args...)
}

// Errorf logs a message at level Error on the standard logger.
func Errorf(format string, args ...interface{}) {
	stdLogger.Errorf(format, args...)
}

// Fatalf logs a message at level Fatal on the standard logger then the process will exit with status set to 1.
func Fatalf(format string, args ...interface{}) {
	stdLogger.Fatalf(format, args...)
}

func With(args ...any) *Logger {
	return stdLogger.WithField(args...)
}

func WithMany(args map[string]any) *Logger {
	return stdLogger.WithField(args)
}

func WithError(err error) *Logger {
	return stdLogger.WithError(err)
}

func WithStacktrace(err error) *Logger {
	return stdLogger.WithStacktrace(err)
}
