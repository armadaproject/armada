package common

import (
	gocontext "context"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func ContextWithDefaultTimeout() (*armadacontext.Context, gocontext.CancelFunc) {
	return armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
}
