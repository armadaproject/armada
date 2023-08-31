package util

import (
	"time"

	"github.com/armadaproject/armada/internal/common/context"
)

func CloseToDeadline(ctx *context.ArmadaContext, tolerance time.Duration) bool {
	deadline, exists := ctx.Deadline()
	return exists && deadline.Before(time.Now().Add(tolerance))
}
