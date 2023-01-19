package podchecks

import (
	"fmt"

	config "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

type Action int

const (
	// Order matters here, actions with higher numbers trump those with lower.
	ActionWait Action = iota
	ActionRetry
	ActionFail
)

func (a Action) String() string {
	switch a {
	case ActionWait:
		return "Wait"
	case ActionRetry:
		return "Retry"
	case ActionFail:
		return "Fail"
	default:
		return fmt.Sprintf("%d", int(a))
	}
}

func mapAction(action config.Action) (Action, error) {
	switch action {
	case config.ActionFail:
		return ActionFail, nil
	case config.ActionRetry:
		return ActionRetry, nil
	default:
		return ActionWait, fmt.Errorf("Invalid action: \"%s\"", action)
	}
}

func maxAction(a, b Action) Action {
	if a > b {
		return a
	}
	return b
}
