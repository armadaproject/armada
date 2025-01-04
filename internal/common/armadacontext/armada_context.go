package armadacontext

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/armadaproject/armada/internal/common/logging"
)

// Context is an extension of Go's context which also includes a logger. This allows us to  pass round a contextual logger
// while retaining type-safety
type Context struct {
	context.Context
	logger *logging.Logger
}

// Debug logs a message at level Debug
func (ctx *Context) Debug(msg string) {
	ctx.logger.Debug(msg)
}

// Info logs a message at level Info
func (ctx *Context) Info(msg string) {
	ctx.logger.Info(msg)
}

// Warn logs a message at level Warn
func (ctx *Context) Warn(msg string) {
	ctx.logger.Warn(msg)
}

// Error logs a message at level Error
func (ctx *Context) Error(msg string) {
	ctx.logger.Error(msg)
}

// Panic logs a message at level Panic
func (ctx *Context) Panic(msg string) {
	ctx.logger.Panic(msg)
}

// Fatal logs a message at level Fatal then the process will exit with status set to 1.
func (ctx *Context) Fatal(msg string) {
	ctx.logger.Fatal(msg)
}

// Debugf logs a message at level Debug.
func (ctx *Context) Debugf(format string, args ...interface{}) {
	ctx.logger.Debugf(format, args...)
}

// Infof logs a message at level Info.
func (ctx *Context) Infof(format string, args ...interface{}) {
	ctx.logger.Infof(format, args...)
}

// Warnf logs a message at level Warn.
func (ctx *Context) Warnf(format string, args ...interface{}) {
	ctx.logger.Warnf(format, args...)
}

// Errorf logs a message at level Error.
func (ctx *Context) Errorf(format string, args ...interface{}) {
	ctx.logger.Errorf(format, args...)
}

// Fatalf logs a message at level Fatal.
func (ctx *Context) Fatalf(format string, args ...interface{}) {
	ctx.logger.Fatalf(format, args...)
}

// Background creates an empty context with a default logger.  It is analogous to context.Background()
func Background() *Context {
	return &Context{
		Context: context.Background(),
		logger:  logging.StdLogger().WithCallerSkip(1),
	}
}

// TODO creates an empty context with a default logger.  It is analogous to context.TODO()
func TODO() *Context {
	return &Context{
		Context: context.TODO(),
		logger:  logging.StdLogger().WithCallerSkip(1),
	}
}

// FromGrpcCtx Converts a context.Context to an armadacontext.Context
func FromGrpcCtx(ctx context.Context) *Context {
	armadaCtx, ok := ctx.(*Context)
	if ok {
		return armadaCtx
	}
	return New(ctx, logging.StdLogger().WithCallerSkip(1))
}

// New returns an  armada context that encapsulates both a go context and a logger
func New(ctx context.Context, log *logging.Logger) *Context {
	return &Context{
		Context: ctx,
		logger:  log,
	}
}

func (ctx *Context) Logger() *logging.Logger {
	return ctx.logger.WithCallerSkip(-1)
}

// WithCancel returns a copy of parent with a new Done channel. It is analogous to context.WithCancel()
func WithCancel(parent *Context) (*Context, context.CancelFunc) {
	c, cancel := context.WithCancel(parent.Context)
	return &Context{
		Context: c,
		logger:  parent.logger,
	}, cancel
}

// WithDeadline returns a copy of the parent context with the deadline adjusted to be no later than d.
// It is analogous to context.WithDeadline()
func WithDeadline(parent *Context, d time.Time) (*Context, context.CancelFunc) {
	c, cancel := context.WithDeadline(parent.Context, d)
	return &Context{
		Context: c,
		logger:  parent.logger,
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
		logger:  parent.logger.WithField(key, val),
	}
}

// WithLogFields returns a copy of parent with the supplied key-values added to the logger
func WithLogFields(parent *Context, fields map[string]any) *Context {
	return &Context{
		Context: parent.Context,
		logger:  parent.logger.WithFields(fields),
	}
}

// WithValue returns a copy of parent in which the value associated with key is
// val. It is analogous to context.WithValue()
func WithValue(parent *Context, key, val any) *Context {
	return &Context{
		Context: context.WithValue(parent, key, val),
		logger:  parent.logger,
	}
}

// ErrGroup returns a new Error Group and an associated Context derived from ctx.
// It is analogous to errgroup.WithContext(ctx)
func ErrGroup(ctx *Context) (*errgroup.Group, *Context) {
	group, goctx := errgroup.WithContext(ctx)
	return group, &Context{
		Context: goctx,
		logger:  ctx.logger,
	}
}
