package armadacontext

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Context is an extension of Go's context which also includes a logger. This allows us to  pass round a contextual logger
// while retaining type-safety
type Context struct {
	context.Context
	Log *logrus.Entry
}

// Background creates an empty context with a default logger.  It is analogous to context.Background()
func Background() *Context {
	return &Context{
		Context: context.Background(),
		Log:     logrus.NewEntry(logrus.New()),
	}
}

// TODO creates an empty context with a default logger.  It is analogous to context.TODO()
func TODO() *Context {
	return &Context{
		Context: context.TODO(),
		Log:     logrus.NewEntry(logrus.New()),
	}
}

// FromGrpcCtx creates a context where the logger is extracted via ctxlogrus's Extract() method.
// Note that this will result in a no-op logger if a logger hasn't already been inserted into the context via ctxlogrus
func FromGrpcCtx(ctx context.Context) *Context {
	log := ctxlogrus.Extract(ctx)
	return New(ctx, log)
}

// New returns an  armada context that encapsulates both a go context and a logger
func New(ctx context.Context, log *logrus.Entry) *Context {
	return &Context{
		Context: ctx,
		Log:     log,
	}
}

// WithCancel returns a copy of parent with a new Done channel. It is analogous to context.WithCancel()
func WithCancel(parent *Context) (*Context, context.CancelFunc) {
	c, cancel := context.WithCancel(parent.Context)
	return &Context{
		Context: c,
		Log:     parent.Log,
	}, cancel
}

// WithDeadline returns a copy of the parent context with the deadline adjusted to be no later than d.
// It is analogous to context.WithDeadline()
func WithDeadline(parent *Context, d time.Time) (*Context, context.CancelFunc) {
	c, cancel := context.WithDeadline(parent.Context, d)
	return &Context{
		Context: c,
		Log:     parent.Log,
	}, cancel
}

// WithTimeout returns WithDeadline(parent, time.Now().Add(timeout)). It is analogous to context.WithTimeout()
func WithTimeout(parent *Context, timeout time.Duration) (*Context, context.CancelFunc) {
	return WithDeadline(parent, time.Now().Add(timeout))
}

// WithLogField returns a copy of parent with the supplied key-value added to the logger
func WithLogField(parent *Context, key string, val interface{}) *Context {
	return &Context{
		Context: parent.Context,
		Log:     parent.Log.WithField(key, val),
	}
}

// WithLogFields returns a copy of parent with the supplied key-values added to the logger
func WithLogFields(parent *Context, fields logrus.Fields) *Context {
	return &Context{
		Context: parent.Context,
		Log:     parent.Log.WithFields(fields),
	}
}

// WithValue returns a copy of parent in which the value associated with key is
// val. It is analogous to context.WithValue()
func WithValue(parent *Context, key, val any) *Context {
	return &Context{
		Context: context.WithValue(parent, key, val),
		Log:     parent.Log,
	}
}

// ErrGroup returns a new Error Group and an associated Context derived from ctx.
// It is analogous to errgroup.WithContext(ctx)
func ErrGroup(ctx *Context) (*errgroup.Group, *Context) {
	group, goctx := errgroup.WithContext(ctx)
	return group, &Context{
		Context: goctx,
		Log:     ctx.Log,
	}
}
