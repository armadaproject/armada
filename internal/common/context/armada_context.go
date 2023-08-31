package context

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type ArmadaContext struct {
	ctx context.Context
	log *logrus.Entry
}

func (c *ArmadaContext) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *ArmadaContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *ArmadaContext) Err() error {
	return c.ctx.Err()
}

func (c *ArmadaContext) Value(key any) any {
	return c.ctx.Value(key)
}

func Background() *ArmadaContext {
	return &ArmadaContext{
		ctx: context.Background(),
		log: logrus.NewEntry(logrus.New()),
	}
}

func TODO() *ArmadaContext {
	return &ArmadaContext{
		ctx: context.TODO(),
		log: logrus.NewEntry(logrus.New()),
	}
}

func New(ctx context.Context, log *logrus.Entry) *ArmadaContext {
	return &ArmadaContext{
		ctx: ctx,
		log: log,
	}
}

func WithCancel(parent *ArmadaContext) (*ArmadaContext, context.CancelFunc) {
	c, cancel := context.WithCancel(parent.ctx)
	return &ArmadaContext{
		ctx: c,
		log: parent.log,
	}, cancel
}

func WithDeadline(parent *ArmadaContext, d time.Time) (*ArmadaContext, context.CancelFunc) {
	c, cancel := context.WithDeadline(parent.ctx, d)
	return &ArmadaContext{
		ctx: c,
		log: parent.log,
	}, cancel
}

func WithTimeout(parent *ArmadaContext, timeout time.Duration) (*ArmadaContext, context.CancelFunc) {
	return WithDeadline(parent, time.Now().Add(timeout))
}

func WithLogField(parent *ArmadaContext, key string, val interface{}) *ArmadaContext {
	return &ArmadaContext{
		ctx: parent,
		log: parent.log.WithField(key, val),
	}
}

func WithValue(parent *ArmadaContext, key, val any) *ArmadaContext {
	return &ArmadaContext{
		ctx: context.WithValue(parent, key, val),
		log: parent.log,
	}
}

func ErrGroup(parent *ArmadaContext) (*errgroup.Group, *ArmadaContext) {
	group, goctx := errgroup.WithContext(parent)
	return group, &ArmadaContext{
		ctx: goctx,
		log: parent.log,
	}
}
