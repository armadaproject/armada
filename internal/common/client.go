package common

import (
	gocontext "context"
	"time"

	"github.com/armadaproject/armada/internal/common/context"
)

func ContextWithDefaultTimeout() (*context.ArmadaContext, gocontext.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}
