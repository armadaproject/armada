package common

import (
	gocontext "context"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

func ContextWithDefaultTimeout() (*armadacontext.ArmadaContext, gocontext.CancelFunc) {
	return armadacontext.WithTimeout(armadacontext.Background(), 10*time.Second)
}
