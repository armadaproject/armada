package armadacontext

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/logging"
)

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

func TestWithLogField(t *testing.T) {
	logger, observedLogs := testLogger()
	ctx := WithLogField(New(context.Background(), logger), "fish", "chips")
	require.Equal(t, context.Background(), ctx.Context)

	ctx.Info("test message")

	// Check the captured log entries
	entries := observedLogs.All()
	require.Len(t, entries, 1, "Expected exactly one log entry")

	// Verify the fields
	logEntry := entries[0]
	assert.Equal(t, "test message", logEntry.Message)
	assert.Equal(t, "chips", logEntry.ContextMap()["fish"])
}

func TestWithLogFields(t *testing.T) {
	logger, observedLogs := testLogger()
	ctx := WithLogFields(New(context.Background(), logger), map[string]any{"fish": "chips", "salt": "pepper"})
	require.Equal(t, context.Background(), ctx.Context)

	ctx.Info("test message")

	// Check the captured log entries
	entries := observedLogs.All()
	require.Len(t, entries, 1, "Expected exactly one log entry")

	// Verify the fields
	logEntry := entries[0]
	assert.Equal(t, "test message", logEntry.Message)
	assert.Equal(t, "chips", logEntry.ContextMap()["fish"])
	assert.Equal(t, "pepper", logEntry.ContextMap()["salt"])
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

func testLogger() (*logging.Logger, *observer.ObservedLogs) {
	core, observedLogs := observer.New(zapcore.DebugLevel)
	baseLogger := zap.New(core)
	logger := logging.FromZap(baseLogger)
	return logger, observedLogs
}
