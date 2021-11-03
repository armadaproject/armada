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
	Regexp      string
	Inverse     bool
	Type        EventType
	GracePeriod time.Duration
	Action      Action
}

type ContainerStatusCheck struct {
	State        ContainerState
	ReasonRegexp string
	Inverse      bool
	GracePeriod  time.Duration
	Action       Action
}
