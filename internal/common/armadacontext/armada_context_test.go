package armadacontext

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/rs/zerolog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/logging"
)

type testLogEntry struct {
	Level        string `json:"level"`
	Message      string `json:"message"`
	CustomField1 string `json:"customField1,omitempty"`
	CustomField2 string `json:"customField2,omitempty"`
}

var defaultLogger = logging.StdLogger().WithField("foo", "bar")

func TestNew(t *testing.T) {
	ctx := New(context.Background(), defaultLogger)
	require.Equal(t, defaultLogger, ctx.logger)
	require.Equal(t, context.Background(), ctx.Context)
}

func TestBackground(t *testing.T) {
	ctx := Background()
	require.Equal(t, ctx.Context, context.Background())
}

func TestTODO(t *testing.T) {
	ctx := TODO()
	require.Equal(t, ctx.Context, context.TODO())
}

func TestLogAtLevel(t *testing.T) {
	tests := map[string]struct {
		logFunction   func(ctx *Context)
		expectedLevel string
		expectedMsg   string
	}{
		"Debug": {
			logFunction: func(ctx *Context) {
				ctx.Debug("test message")
			},
			expectedMsg:   "test message",
			expectedLevel: "debug",
		},
		"Debugf": {
			logFunction: func(ctx *Context) {
				ctx.Debugf("test message %d", 1)
			},
			expectedMsg:   "test message 1",
			expectedLevel: "debug",
		},
		"Info": {
			logFunction: func(ctx *Context) {
				ctx.Info("test message")
			},
			expectedMsg:   "test message",
			expectedLevel: "info",
		},
		"Infof": {
			logFunction: func(ctx *Context) {
				ctx.Infof("test message %d", 1)
			},
			expectedMsg:   "test message 1",
			expectedLevel: "info",
		},
		"Warn": {
			logFunction: func(ctx *Context) {
				ctx.Warn("test message")
			},
			expectedMsg:   "test message",
			expectedLevel: "warn",
		},
		"Warnf": {
			logFunction: func(ctx *Context) {
				ctx.Warnf("test message %d", 1)
			},
			expectedMsg:   "test message 1",
			expectedLevel: "warn",
		},
		"Error": {
			logFunction: func(ctx *Context) {
				ctx.Errorf("test message")
			},
			expectedMsg:   "test message",
			expectedLevel: "error",
		},
		"Errorf": {
			logFunction: func(ctx *Context) {
				ctx.Errorf("test message %d", 1)
			},
			expectedMsg:   "test message 1",
			expectedLevel: "error",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			logger, buf := testLogger()
			ctx := New(context.Background(), logger)
			tc.logFunction(ctx)
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

func TestWithLogField(t *testing.T) {
	logger, buf := testLogger()
	ctx := WithLogField(New(context.Background(), logger), "customField1", "foo")

	// Ensure the context is correctly set
	require.Equal(t, context.Background(), ctx.Context)

	ctx.Info("test message")
	require.NotEmpty(t, buf.Bytes(), "Expected log entry in buffer")

	var entry testLogEntry
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err, "Failed to unmarshal log entry")

	assert.Equal(t, "test message", entry.Message)
	assert.Equal(t, "foo", entry.CustomField1)
	assert.Equal(t, "info", entry.Level)
}

func TestWithLogFields(t *testing.T) {
	logger, buf := testLogger()

	ctx := WithLogFields(
		New(context.Background(), logger),
		map[string]interface{}{
			"customField1": "bar",
			"customField2": "baz",
		},
	)

	// Ensure the context is correctly set
	require.Equal(t, context.Background(), ctx.Context)

	ctx.Info("test message")
	require.NotEmpty(t, buf.Bytes(), "Expected log entry in buffer")

	var entry testLogEntry
	err := json.Unmarshal(buf.Bytes(), &entry)
	require.NoError(t, err, "Failed to unmarshal log entry")

	assert.Equal(t, "test message", entry.Message)
	assert.Equal(t, "bar", entry.CustomField1)
	assert.Equal(t, "baz", entry.CustomField2)
	assert.Equal(t, "info", entry.Level)
}

func TestWithTimeout(t *testing.T) {
	ctx, _ := WithTimeout(Background(), 100*time.Millisecond)
	testDeadline(t, ctx)
}

func TestWithDeadline(t *testing.T) {
	ctx, _ := WithDeadline(Background(), time.Now().Add(100*time.Millisecond))
	testDeadline(t, ctx)
}

func TestWithValue(t *testing.T) {
	ctx := WithValue(Background(), "foo", "bar")
	require.Equal(t, "bar", ctx.Value("foo"))
}

func testDeadline(t *testing.T, c *Context) {
	t.Helper()
	d := quiescent(t)
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		t.Fatalf("context not timed out after %v", d)
	case <-c.Done():
	}
	if e := c.Err(); e != context.DeadlineExceeded {
		t.Errorf("c.Err() == %v; want %v", e, context.DeadlineExceeded)
	}
}

func quiescent(t *testing.T) time.Duration {
	deadline, ok := t.Deadline()
	if !ok {
		return 5 * time.Second
	}

	const arbitraryCleanupMargin = 1 * time.Second
	return time.Until(deadline) - arbitraryCleanupMargin
}

// testLogger sets up a Zerolog logger that writes to a buffer for testing
func testLogger() (*logging.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	baseLogger := zerolog.New(&buf).Level(zerolog.DebugLevel).With().Timestamp().Logger()
	logger := logging.FromZerolog(baseLogger)
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
}
