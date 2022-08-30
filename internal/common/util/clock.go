package util

import "time"

type Clock interface {
	Now() time.Time
}

type DefaultClock struct{}

func (c *DefaultClock) Now() time.Time { return time.Now() }

type DummyClock struct {
	T time.Time
}

func (c *DummyClock) Now() time.Time {
	return c.T
}
