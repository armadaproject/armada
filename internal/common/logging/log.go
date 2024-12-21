package logging

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

func Debug(msg string) {
	slog.Default().Debug(msg)
}

func Debugf(format string, args ...any) {
	slog.Debug(fmt.Sprintf(format, args))
}

func Infof(format string, args ...any) {
	slog.Info(fmt.Sprintf(format, args))
}

func Info(msg string) {
	l := slog.Default()
	if !l.Enabled(context.Background(), slog.LevelInfo) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // skip [Callers, Infof]
	r := slog.NewRecord(time.Now(), slog.LevelInfo, fmt.Sprintf(format, args...), pcs[0])
	_ = l.Handler().Handle(context.Background(), r)
	slog.Info(msg)
}

func Warn(msg string) {
	slog.Warn(msg)
}

func Warnf(format string, args ...any) {
	slog.Warn(fmt.Sprintf(format, args))
}

func Error(msg string) {
	slog.Error(msg)
}

func Errorf(format string, args ...any) {
	slog.Error(fmt.Sprintf(format, args))
}

func Fatal(msg string) {
	slog.Error(msg)
}

func Fatalf(format string, args ...any) {
	slog.Error(fmt.Sprintf(format, args))
}

func With(key, value any) Logger {
	return NewLogger().With(key, value)
}

func WithError(err error) Logger {
	return NewLogger().With("error", err.Error())
}

func WithStacktrace(err error) Logger {
	logger := WithError(err)
	if stackErr, ok := err.(stackTracer); ok {
		logger = logger.With("stacktrace", fmt.Sprintf("%s", stackErr.StackTrace()))
	}
	return logger
}
