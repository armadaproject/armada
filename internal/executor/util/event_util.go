package util

import v1 "k8s.io/api/core/v1"

const (
	EventReasonPreempted = "Preempted"
)

func IsPreemptedEvent(event *v1.Event) bool {
	return event.Reason == EventReasonPreempted
}
