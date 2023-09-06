package armadacontext

import (
	"context"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var defaultLogger = logrus.WithField("foo", "bar")

func TestNew(t *testing.T) {
	ctx := New(context.Background(), defaultLogger)
	require.Equal(t, defaultLogger, ctx.Log)
	require.Equal(t, context.Background(), ctx.Context)
}

func TestFromGrpcContext(t *testing.T) {
	grpcCtx := ctxlogrus.ToContext(context.Background(), defaultLogger)
	ctx := FromGrpcCtx(grpcCtx)
	require.Equal(t, grpcCtx, ctx.Context)
	require.Equal(t, defaultLogger, ctx.Log)
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
	ctx := WithLogField(Background(), "fish", "chips")
	require.Equal(t, context.Background(), ctx.Context)
	require.Equal(t, logrus.Fields{"fish": "chips"}, ctx.Log.Data)
}

func TestWithLogFields(t *testing.T) {
	ctx := WithLogFields(Background(), logrus.Fields{"fish": "chips", "salt": "pepper"})
	require.Equal(t, context.Background(), ctx.Context)
	require.Equal(t, logrus.Fields{"fish": "chips", "salt": "pepper"}, ctx.Log.Data)
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
