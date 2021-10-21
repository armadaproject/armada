package podchecks

import (
	config "github.com/G-Research/armada/internal/executor/configuration/podchecks"
)

type Action string

const (
	ActionFail  Action = "Fail"
	ActionRetry Action = "Retry"
	ActionWait  Action = "Wait"
)

func mapAction(action config.Action) Action {
	switch action {
	case config.ActionFail:
		return ActionFail
	case config.ActionRetry:
		return ActionRetry
	default:
		return ActionWait
	}
}
