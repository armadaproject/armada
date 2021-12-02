package util

import (
	"context"
	"time"
)

func CloseToDeadline(ctx context.Context) bool {
	deadline, exists := ctx.Deadline()
	return exists && deadline.Before(time.Now().Add(time.Second))
}
