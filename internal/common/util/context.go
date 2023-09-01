package util

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func CloseToDeadline(ctx *armadacontext.Context, tolerance time.Duration) bool {
	deadline, exists := ctx.Deadline()
	return exists && deadline.Before(time.Now().Add(tolerance))
}
