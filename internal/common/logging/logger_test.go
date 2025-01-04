package logging

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestWithField(t *testing.T) {

	logger, observedLogs := testLogger()

	newLogger := logger.WithField("foo", "bar")
	newLogger.Info("test message")

	// Check the captured log entries
	entries := observedLogs.All()
	require.Len(t, entries, 1, "Expected exactly one log entry")

	// Verify the fields
	logEntry := entries[0]
	assert.Equal(t, "test message", logEntry.Message)
	assert.Equal(t, "bar", logEntry.ContextMap()["foo"])
}

func TestWithFields(t *testing.T) {

	logger, observedLogs := testLogger()

	newLogger := logger.WithFields(map[string]any{
		"user":   "test_user",
		"action": "test_action",
	})
	newLogger.Info("test message")

	// Check the captured log entries
	entries := observedLogs.All()
	require.Len(t, entries, 1, "Expected exactly one log entry")

	// Verify the fields
	logEntry := entries[0]
	assert.Equal(t, "test message", logEntry.Message)
	assert.Equal(t, "test_user", logEntry.ContextMap()["user"])
	assert.Equal(t, "test_action", logEntry.ContextMap()["action"])
}

func TestWithError(t *testing.T) {
	logger, observedLogs := testLogger()

	err := errors.New("test error")

	newLogger := logger.WithError(err)
	newLogger.Info("test message")

	// Check the captured log entries
	entries := observedLogs.All()
	require.Len(t, entries, 1, "Expected exactly one log entry")

	// Verify the fields
	logEntry := entries[0]
	assert.Equal(t, "test message", logEntry.Message)
	assert.Equal(t, "test error", logEntry.ContextMap()["error"])
}

func TestWithStacktrace(t *testing.T) {
	logger, observedLogs := testLogger()

	err := errors.WithStack(errors.New("test error"))

	newLogger := logger.WithStacktrace(err)
	newLogger.Info("test message")

	// Check the captured log entries
	entries := observedLogs.All()
	require.Len(t, entries, 1, "Expected exactly one log entry")

	// Verify the fields
	logEntry := entries[0]
	assert.Equal(t, "test message", logEntry.Message)
	assert.Equal(t, "test error", logEntry.ContextMap()["error"])
	assert.Equal(t, err.(stackTracer).StackTrace(), logEntry.ContextMap()["stacktrace"])
}

func testLogger() (*Logger, *observer.ObservedLogs) {
	core, observedLogs := observer.New(zapcore.DebugLevel)
	baseLogger := zap.New(core).Sugar()
	logger := &Logger{underlying: baseLogger}
	return logger, observedLogs
}
