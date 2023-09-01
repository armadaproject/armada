package armadacontext

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Context struct {
	Ctx context.Context
	Log *logrus.Entry
}

func (c *Context) Deadline() (deadline time.Time, ok bool) {
	return c.Ctx.Deadline()
}

func (c *Context) Done() <-chan struct{} {
	return c.Ctx.Done()
}

func (c *Context) Err() error {
	return c.Ctx.Err()
}

func (c *Context) Value(key any) any {
	return c.Ctx.Value(key)
}

func Background() *Context {
	return &Context{
		Ctx: context.Background(),
		Log: logrus.NewEntry(logrus.New()),
	}
}

func TODO() *Context {
	return &Context{
		Ctx: context.TODO(),
		Log: logrus.NewEntry(logrus.New()),
	}
}

func FromGrpcContext(context context.Context) *Context {
	return New(context, ctxlogrus.Extract(context))
}

func New(ctx context.Context, log *logrus.Entry) *Context {
	return &Context{
		Ctx: ctx,
		Log: log,
	}
}

func WithCancel(parent *Context) (*Context, context.CancelFunc) {
	c, cancel := context.WithCancel(parent.Ctx)
	return &Context{
		Ctx: c,
		Log: parent.Log,
	}, cancel
}

func WithDeadline(parent *Context, d time.Time) (*Context, context.CancelFunc) {
	c, cancel := context.WithDeadline(parent.Ctx, d)
	return &Context{
		Ctx: c,
		Log: parent.Log,
	}, cancel
}

func WithTimeout(parent *Context, timeout time.Duration) (*Context, context.CancelFunc) {
	return WithDeadline(parent, time.Now().Add(timeout))
}

func WithLogField(parent *Context, key string, val interface{}) *Context {
	return &Context{
		Ctx: parent,
		Log: parent.Log.WithField(key, val),
	}
}

func WithLogFields(parent *Context, fields map[string]interface{}) *Context {
	return &Context{
		Ctx: parent,
		Log: parent.Log.WithFields(fields),
	}
}

func WithValue(parent *Context, key, val any) *Context {
	return &Context{
		Ctx: context.WithValue(parent, key, val),
		Log: parent.Log,
	}
}

func ErrGroup(parent *Context) (*errgroup.Group, *Context) {
	group, goctx := errgroup.WithContext(parent)
	return group, &Context{
		Ctx: goctx,
		Log: parent.Log,
	}
}
