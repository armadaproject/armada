package util

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func CloseToDeadline(ctx *armadacontext.ArmadaContext, tolerance time.Duration) bool {
	deadline, exists := ctx.Deadline()
	return exists && deadline.Before(time.Now().Add(tolerance))
}
