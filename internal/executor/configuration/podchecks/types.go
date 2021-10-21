package podchecks

import (
	"time"
)

type Action string

const (
	ActionFail  Action = "Fail"
	ActionRetry Action = "Retry"
)

type ContainerState string

const (
	ContainerStateWaiting ContainerState = "Waiting"
)

type Checks struct {
	Events            []EventCheck
	ContainerStatuses []ContainerStatusCheck
}

type EventCheck struct {
	Regexp string
	Action Action
}

type ContainerStatusCheck struct {
	State        ContainerState
	ReasonRegexp string
	Timeout      time.Duration
	Action       Action
}
