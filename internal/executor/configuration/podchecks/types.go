package podchecks

import (
	"time"

	v1 "k8s.io/api/core/v1"
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

type EventType string

const (
	EventTypeWarning EventType = EventType(v1.EventTypeWarning)
	EventTypeNormal  EventType = EventType(v1.EventTypeNormal)
)

type Checks struct {
	Events            []EventCheck
	ContainerStatuses []ContainerStatusCheck
}

type EventCheck struct {
	Regexp  string
	Inverse bool
	Action  Action
	Type    EventType
}

type ContainerStatusCheck struct {
	State        ContainerState
	ReasonRegexp string
	Timeout      time.Duration
	Action       Action
}
