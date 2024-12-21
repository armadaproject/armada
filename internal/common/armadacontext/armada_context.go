package armadacontext

import (
	"context"
	"fmt"
	"github.com/armadaproject/armada/internal/common/logging"
	"time"

	"golang.org/x/sync/errgroup"
)

// Context is an extension of Go's context which also includes a Logger. This allows us to  pass round a contextual Logger
// while retaining type-safety
type Context struct {
	context.Context
	Logger logging.Logger
}

// Background creates an empty context with a default Logger.  It is analogous to context.Background()
func Background() *Context {
	return &Context{
		Context: context.Background(),
		Logger:  logging.NewLogger(),
	}
}

// TODO creates an empty context with a default Logger.  It is analogous to context.TODO()
func TODO() *Context {
	return &Context{
		Context: context.TODO(),
		Logger:  logging.NewLogger(),
	}
}

// FromGrpcCtx creates a context where the Logger is extracted via ctxlogrus's Extract() method.
// Note that this will result in a no-op Logger if a Logger hasn't already been inserted into the context via ctxlogrus
func FromGrpcCtx(ctx context.Context) *Context {
	panic("not implemented!")
	//armadaCtx, ok := ctx.(*Context)
	//if ok {
	//	return armadaCtx
	//}
	//log := ctxlogrus.Extract(ctx)
	//return New(ctx, log)
}

// New returns an  armada context that encapsulates both a go context and a Logger
func New(ctx context.Context, log logging.Logger) *Context {
	return &Context{
		Context: ctx,
		Logger:  log,
	}
}

// WithCancel returns a copy of parent with a new Done channel. It is analogous to context.WithCancel()
func WithCancel(parent *Context) (*Context, context.CancelFunc) {
	c, cancel := context.WithCancel(parent.Context)
	return &Context{
		Context: c,
		Logger:  parent.Logger,
	}, cancel
}

// WithDeadline returns a copy of the parent context with the deadline adjusted to be no later than d.
// It is analogous to context.WithDeadline()
func WithDeadline(parent *Context, d time.Time) (*Context, context.CancelFunc) {
	c, cancel := context.WithDeadline(parent.Context, d)
	return &Context{
		Context: c,
		Logger:  parent.Logger,
	}, cancel
}

// WithTimeout returns WithDeadline(parent, time.Now().Add(timeout)). It is analogous to context.WithTimeout()
func WithTimeout(parent *Context, timeout time.Duration) (*Context, context.CancelFunc) {
	return WithDeadline(parent, time.Now().Add(timeout))
}

// WithLogField returns a copy of parent with the supplied key-value added to the Logger
func WithLogField(parent *Context, key string, val any) *Context {
	return &Context{
		Context: parent.Context,
		Logger:  parent.Logger.With(key, val),
	}
}

// WithValue returns a copy of parent in which the value associated with key is
// val. It is analogous to context.WithValue()
func WithValue(parent *Context, key, val any) *Context {
	return &Context{
		Context: context.WithValue(parent, key, val),
		Logger:  parent.Logger,
	}
}

// ErrGroup returns a new Error Group and an associated Context derived from ctx.
// It is analogous to errgroup.WithContext(ctx)
func ErrGroup(ctx *Context) (*errgroup.Group, *Context) {
	group, goctx := errgroup.WithContext(ctx)
	return group, &Context{
		Context: goctx,
		Logger:  ctx.Logger,
	}
}

func (ctx *Context) Debug(msg string) {
	ctx.Logger.Debug(msg)
}

func (ctx *Context) Debugf(format string, args ...any) {
	ctx.Logger.Debug(fmt.Sprintf(format, args))
}

func (ctx *Context) Infof(format string, args ...any) {
	ctx.Logger.Info(fmt.Sprintf(format, args))
}

func (ctx *Context) Info(msg string) {
	ctx.Logger.Info(msg)
}

func (ctx *Context) Warn(msg string) {
	ctx.Logger.Warn(msg)
}

func (ctx *Context) Warnf(format string, args ...any) {
	ctx.Logger.Warn(fmt.Sprintf(format, args))
}

func (ctx *Context) Error(msg string) {
	ctx.Logger.Error(msg)
}

func (ctx *Context) Errorf(format string, args ...any) {
	ctx.Logger.Error(fmt.Sprintf(format, args))
}

func (ctx *Context) Fatal(msg string) {
	ctx.Logger.Fatal(msg)
}

func (ctx *Context) Fatalf(format string, args ...any) {
	ctx.Logger.Fatalf(fmt.Sprintf(format, args))
}
