package task

import "time"

type ScheduledTask interface {
	Execute()
	GetInterval() time.Duration
}
