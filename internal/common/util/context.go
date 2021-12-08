package util

import (
	"context"
	"time"
)

func CloseToDeadline(ctx context.Context, tolerance time.Duration) bool {
	deadline, exists := ctx.Deadline()
	return exists && deadline.Before(time.Now().Add(tolerance))
}
