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
	EventTypeNormal = EventType(v1.EventTypeNormal)
)

type Checks struct {
	Events            []EventCheck
	ContainerStatuses []ContainerStatusCheck
	// If both events and status are not found from pod
	// for this amount of time, we assume we can retry the pod.
	TimeWithoutEventsOrStatus time.Duration
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
