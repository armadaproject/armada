package armadacontext

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type ArmadaContext struct {
	Ctx context.Context
	Log *logrus.Entry
}

func (c *ArmadaContext) Deadline() (deadline time.Time, ok bool) {
	return c.Ctx.Deadline()
}

func (c *ArmadaContext) Done() <-chan struct{} {
	return c.Ctx.Done()
}

func (c *ArmadaContext) Err() error {
	return c.Ctx.Err()
}

func (c *ArmadaContext) Value(key any) any {
	return c.Ctx.Value(key)
}

func Background() *ArmadaContext {
	return &ArmadaContext{
		Ctx: context.Background(),
		Log: logrus.NewEntry(logrus.New()),
	}
}

func TODO() *ArmadaContext {
	return &ArmadaContext{
		Ctx: context.TODO(),
		Log: logrus.NewEntry(logrus.New()),
	}
}

func FromGrpcContext(context context.Context) *ArmadaContext {
	return New(context, ctxlogrus.Extract(context))
}

func New(ctx context.Context, log *logrus.Entry) *ArmadaContext {
	return &ArmadaContext{
		Ctx: ctx,
		Log: log,
	}
}

func WithCancel(parent *ArmadaContext) (*ArmadaContext, context.CancelFunc) {
	c, cancel := context.WithCancel(parent.Ctx)
	return &ArmadaContext{
		Ctx: c,
		Log: parent.Log,
	}, cancel
}

func WithDeadline(parent *ArmadaContext, d time.Time) (*ArmadaContext, context.CancelFunc) {
	c, cancel := context.WithDeadline(parent.Ctx, d)
	return &ArmadaContext{
		Ctx: c,
		Log: parent.Log,
	}, cancel
}

func WithTimeout(parent *ArmadaContext, timeout time.Duration) (*ArmadaContext, context.CancelFunc) {
	return WithDeadline(parent, time.Now().Add(timeout))
}

func WithLogField(parent *ArmadaContext, key string, val interface{}) *ArmadaContext {
	return &ArmadaContext{
		Ctx: parent,
		Log: parent.Log.WithField(key, val),
	}
}

func WithLogFields(parent *ArmadaContext, fields map[string]interface{}) *ArmadaContext {
	return &ArmadaContext{
		Ctx: parent,
		Log: parent.Log.WithFields(fields),
	}
}

func WithValue(parent *ArmadaContext, key, val any) *ArmadaContext {
	return &ArmadaContext{
		Ctx: context.WithValue(parent, key, val),
		Log: parent.Log,
	}
}

func ErrGroup(parent *ArmadaContext) (*errgroup.Group, *ArmadaContext) {
	group, goctx := errgroup.WithContext(parent)
	return group, &ArmadaContext{
		Ctx: goctx,
		Log: parent.Log,
	}
}
