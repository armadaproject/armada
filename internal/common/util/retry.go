package util

import (
	"github.com/armadaproject/armada/internal/common/context"
)

func RetryUntilSuccess(ctx *context.ArmadaContext, performAction func() error, onError func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := performAction()
			if err == nil {
				return
			} else {
				onError(err)
			}
		}
	}
}
