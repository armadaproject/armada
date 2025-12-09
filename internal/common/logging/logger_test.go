package logging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testLogEntry struct {
	Level        string `json:"level"`
	Message      string `json:"message"`
	CustomField1 string `json:"customField1,omitempty"`
	CustomField2 string `json:"customField2,omitempty"`
	Error        string `json:"error,omitempty"`
	Stacktrace   string `json:"stacktrace,omitempty"`
}

func TestWithField(t *testing.T) {
	logger, buf := testLogger()
	logger = logger.WithField("customField1", "foo")

	logger.Info("test message")

	assertLogLineExpected(
		t,
		&testLogEntry{
			Level:        "info",
			Message:      "test message",
			CustomField1: "foo",
		},
		buf,
	)
}

func TestWithFields(t *testing.T) {
	logger, buf := testLogger()
	logger = logger.WithFields(
		map[string]any{"customField1": "bar", "customField2": "baz"},
	)

	logger.Info("test message")

	assertLogLineExpected(
		t,
		&testLogEntry{
			Level:        "info",
			Message:      "test message",
			CustomField1: "bar",
			CustomField2: "baz",
		},
		buf,
	)
}

func TestWithError(t *testing.T) {
	logger, buf := testLogger()
	err := errors.New("test error")
	logger = logger.WithError(err)

	logger.Info("test message")

	assertLogLineExpected(
		t,
		&testLogEntry{
			Level:   "info",
			Message: "test message",
			Error:   "test error",
		},
		buf,
	)
}

func TestWithStacktrace(t *testing.T) {
	errorsNew := errors.New("test error")
	errorsErrorf := errors.Errorf("test error")
	errorsWithStack := errors.WithStack(fmt.Errorf("inner error"))
	innerErrorWithStackTrace := errors.Errorf("inner error")
	errorsWithMessageNested := errors.WithMessage(innerErrorWithStackTrace, "outer message")
	errorsWithMessageNested2 := errors.WithMessage(errorsWithMessageNested, "even outer message")
	tests := map[string]struct {
		err         error
		expectedLog *testLogEntry
	}{
		"errors.New": {
			err: errorsNew,
			expectedLog: &testLogEntry{
				Level:      "info",
				Message:    "test message",
				Error:      "test error",
				Stacktrace: fmt.Sprintf("%v", errorsNew.(stackTracer).StackTrace()),
			},
		},
		"errors.ErrorF": {
			err: errorsErrorf,
			expectedLog: &testLogEntry{
				Level:      "info",
				Message:    "test message",
				Error:      "test error",
				Stacktrace: fmt.Sprintf("%v", errorsErrorf.(stackTracer).StackTrace()),
			},
		},
		"errors.WithStack": {
			err: errorsWithStack,
			expectedLog: &testLogEntry{
				Level:      "info",
				Message:    "test message",
				Error:      "inner error",
				Stacktrace: fmt.Sprintf("%v", errorsWithStack.(stackTracer).StackTrace()),
			},
		},
		"errors.WithMessage - nested": {
			err: errorsWithMessageNested,
			expectedLog: &testLogEntry{
				Level:      "info",
				Message:    "test message",
				Error:      "outer message: inner error",
				Stacktrace: fmt.Sprintf("%v", innerErrorWithStackTrace.(stackTracer).StackTrace()),
			},
		},
		"errors.WithMessage - nested 2": {
			err: errorsWithMessageNested2,
			expectedLog: &testLogEntry{
				Level:      "info",
				Message:    "test message",
				Error:      "even outer message: outer message: inner error",
				Stacktrace: fmt.Sprintf("%v", innerErrorWithStackTrace.(stackTracer).StackTrace()),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			logger, buf := testLogger()
			logger = logger.WithStacktrace(tc.err)
			logger.Info("test message")

			assertLogLineExpected(
				t,
				tc.expectedLog,
				buf,
			)
		})
	}
}

func TestLogAtLevel(t *testing.T) {
	tests := map[string]struct {
		logFunction   func(logger *Logger)
		expectedLevel string
		expectedMsg   string
	}{
		"Debug": {
			logFunction: func(l *Logger) {
				l.Debug("test message")
			},
			expectedMsg:   "test message",
			expectedLevel: "debug",
		},
		"Debugf": {
			logFunction: func(l *Logger) {
				l.Debugf("test message %d", 1)
			},
			expectedMsg:   "test message 1",
			expectedLevel: "debug",
		},
		"Info": {
			logFunction: func(l *Logger) {
				l.Info("test message")
			},
			expectedMsg:   "test message",
			expectedLevel: "info",
		},
		"Infof": {
			logFunction: func(l *Logger) {
				l.Infof("test message %d", 1)
			},
			expectedMsg:   "test message 1",
			expectedLevel: "info",
		},
		"Warn": {
			logFunction: func(l *Logger) {
				l.Warn("test message")
			},
			expectedMsg:   "test message",
			expectedLevel: "warn",
		},
		"Warnf": {
			logFunction: func(l *Logger) {
				l.Warnf("test message %d", 1)
			},
			expectedMsg:   "test message 1",
			expectedLevel: "warn",
		},
		"Error": {
			logFunction: func(l *Logger) {
				l.Errorf("test message")
			},
			expectedMsg:   "test message",
			expectedLevel: "error",
		},
		"Errorf": {
			logFunction: func(l *Logger) {
				l.Errorf("test message %d", 1)
			},
			expectedMsg:   "test message 1",
			expectedLevel: "error",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			logger, buf := testLogger()
			tc.logFunction(logger)
			assertLogLineExpected(
				t,
				&testLogEntry{
					Level:   tc.expectedLevel,
					Message: tc.expectedMsg,
				},
				buf,
			)
		})
	}
}

// testLogger sets up a Zerolog logger that writes to a buffer for testing
func testLogger() (*Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	baseLogger := zerolog.New(&buf).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	logger := FromZerolog(baseLogger)
	return logger, &buf
}

func assertLogLineExpected(t *testing.T, expected *testLogEntry, logOutput *bytes.Buffer) {
	var entry testLogEntry
	err := json.Unmarshal(logOutput.Bytes(), &entry)
	require.NoError(t, err, "Failed to unmarshal log entry")

	assert.Equal(t, expected.Message, entry.Message)
	assert.Equal(t, expected.Level, entry.Level)
	assert.Equal(t, expected.CustomField1, entry.CustomField1)
	assert.Equal(t, expected.CustomField2, entry.CustomField2)
	assert.Equal(t, expected.Error, entry.Error)
	assert.Equal(t, expected.Stacktrace, entry.Stacktrace)
}
