package common

import (
	"context"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func ContextWithDefaultTimeout() (*armadacontext.Context, context.CancelFunc) {
	return armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
}
