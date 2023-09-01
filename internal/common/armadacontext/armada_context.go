package armadacontext

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Context struct {
	context.Context
	Log *logrus.Entry
}

func Background() *Context {
	return &Context{
		Context: context.Background(),
		Log:     logrus.NewEntry(logrus.New()),
	}
}

func TODO() *Context {
	return &Context{
		Context: context.TODO(),
		Log:     logrus.NewEntry(logrus.New()),
	}
}

func FromGrpcContext(ctx context.Context) *Context {
	log := ctxlogrus.Extract(ctx)
	return New(ctx, log)
}

func New(ctx context.Context, log *logrus.Entry) *Context {
	return &Context{
		Context: ctx,
		Log:     log,
	}
}

func WithCancel(parent *Context) (*Context, context.CancelFunc) {
	c, cancel := context.WithCancel(parent.Context)
	return &Context{
		Context: c,
		Log:     parent.Log,
	}, cancel
}

func WithDeadline(parent *Context, d time.Time) (*Context, context.CancelFunc) {
	c, cancel := context.WithDeadline(parent.Context, d)
	return &Context{
		Context: c,
		Log:     parent.Log,
	}, cancel
}

func WithTimeout(parent *Context, timeout time.Duration) (*Context, context.CancelFunc) {
	return WithDeadline(parent, time.Now().Add(timeout))
}

func WithLogField(parent *Context, key string, val interface{}) *Context {
	return &Context{
		Context: parent,
		Log:     parent.Log.WithField(key, val),
	}
}

func WithLogFields(parent *Context, fields map[string]interface{}) *Context {
	return &Context{
		Context: parent,
		Log:     parent.Log.WithFields(fields),
	}
}

func WithValue(parent *Context, key, val any) *Context {
	return &Context{
		Context: context.WithValue(parent, key, val),
		Log:     parent.Log,
	}
}

func ErrGroup(parent *Context) (*errgroup.Group, *Context) {
	group, goctx := errgroup.WithContext(parent)
	return group, &Context{
		Context: goctx,
		Log:     parent.Log,
	}
}
