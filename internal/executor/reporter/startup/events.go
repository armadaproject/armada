package startup

import (
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Event reasons read here. Pulled covers both a completed pull and an image that was already on the
// node, so those two are told apart by message rather than reason. Scheduled is kube-scheduler's,
// the rest are the kubelet's.
const (
	pulledReason    = "Pulled"
	createdReason   = "Created"
	startedReason   = "Started"
	scheduledReason = "Scheduled"
)

// The kubelet attributes a container event to its container through the involvedObject fieldPath,
// built by GenerateContainerRef, which uses distinct paths for init and app containers. Matching on
// it rather than on the container name keeps an init container and an app container that share a
// name apart.
func initContainerKey(name string) string {
	return "spec.initContainers{" + name + "}"
}

func appContainerFieldKey(name string) string {
	return "spec.containers{" + name + "}"
}

type containerEvents map[string]*v1.Event

func groupEventsByContainer(podEvents []*v1.Event) map[string]containerEvents {
	indexed := map[string]containerEvents{}
	for _, event := range podEvents {
		if event == nil || event.InvolvedObject.FieldPath == "" {
			continue
		}
		events, exists := indexed[event.InvolvedObject.FieldPath]
		if !exists {
			events = containerEvents{}
			indexed[event.InvolvedObject.FieldPath] = events
		}
		// Keep the earliest of a repeated reason: a pull that failed and was retried emits several
		// Pulling events, and the first is when the container started waiting.
		existing, seen := events[event.Reason]
		if !seen || eventTime(event).Before(eventTime(existing)) {
			events[event.Reason] = event
		}
	}
	return indexed
}

func (e containerEvents) timeOf(reason string) *time.Time {
	event, exists := e[reason]
	if !exists {
		return nil
	}
	at := eventTime(event)
	if at.IsZero() {
		return nil
	}
	return &at
}

func (e containerEvents) getImagePullInfo() *armadaevents.ImagePullInfo {
	event, exists := e[pulledReason]
	if !exists {
		return nil
	}
	return parsePulledMessage(event.Message)
}

// eventTime handles both event APIs, which matters because the two sources read here use different
// ones. The kubelet's legacy recorder sets FirstTimestamp; kube-scheduler's uses the newer
// events.k8s.io recorder, which sets only EventTime and leaves FirstTimestamp empty in the core/v1
// view. EventTime is a MicroTime, so a scheduler event is finer-grained than any pod field.
func eventTime(event *v1.Event) time.Time {
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	return event.LastTimestamp.Time
}

// firstEventTimeWithReason is the earliest time any of the pod's events carries a reason, or nil
// when none does.
func firstEventTimeWithReason(podEvents []*v1.Event, reason string) *time.Time {
	var earliest *time.Time
	for _, event := range podEvents {
		if event == nil || event.Reason != reason {
			continue
		}
		at := eventTime(event)
		if at.IsZero() {
			continue
		}
		if earliest == nil || at.Before(*earliest) {
			earliest = new(at)
		}
	}
	return earliest
}

func eventsInTimeOrder(podEvents []*v1.Event) []*v1.Event {
	ordered := make([]*v1.Event, 0, len(podEvents))
	for _, event := range podEvents {
		if event != nil {
			ordered = append(ordered, event)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		return eventTime(ordered[i]).Before(eventTime(ordered[j]))
	})
	return ordered
}
