package startup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The two event APIs populate different timestamp fields, and both sources read here are in play:
// the kubelet's legacy recorder sets FirstTimestamp, kube-scheduler's events.k8s.io recorder sets
// only EventTime. Reading just one field would silently lose one of them.
func TestEventTime(t *testing.T) {
	tests := map[string]struct {
		event    *v1.Event
		expected time.Time
	}{
		"kubelet style, FirstTimestamp": {
			event:    &v1.Event{FirstTimestamp: baselineWithOffset(5)},
			expected: baselineWithOffset(5).Time,
		},
		"scheduler style, EventTime only": {
			event:    &v1.Event{EventTime: metav1.NewMicroTime(baselineWithOffset(7).Time)},
			expected: baselineWithOffset(7).Time,
		},
		"FirstTimestamp wins when both are set": {
			event:    &v1.Event{FirstTimestamp: baselineWithOffset(5), EventTime: metav1.NewMicroTime(baselineWithOffset(7).Time)},
			expected: baselineWithOffset(5).Time,
		},
		"falls back to LastTimestamp": {
			event:    &v1.Event{LastTimestamp: baselineWithOffset(9)},
			expected: baselineWithOffset(9).Time,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, eventTime(tc.event))
		})
	}
}

// EventTime is a MicroTime, so a scheduler event is finer grained than anything on the pod.
func TestEventTime_PreservesSubSecondPrecision(t *testing.T) {
	precise := baseTime().Add(1500 * time.Millisecond)

	actual := eventTime(&v1.Event{EventTime: metav1.NewMicroTime(precise)})

	assert.Equal(t, precise, actual)
}

func TestIndexEventsByContainer_KeepsEarliestOfARepeatedReason(t *testing.T) {
	events := []*v1.Event{
		podEvent("spec.containers{main}", "Pulling", "retry", baselineWithOffset(20)),
		podEvent("spec.containers{main}", "Pulling", "first attempt", baselineWithOffset(10)),
	}

	indexed := groupEventsByContainer(events)

	assert.Equal(t, baselineWithOffset(10).Time, *indexed["spec.containers{main}"].timeOf("Pulling"))
}

// Events without a fieldPath are not attributable to a container - the scheduler's Scheduled event
// is one - so they must not be indexed under an empty key and picked up by a container lookup.
func TestIndexEventsByContainer_SkipsEventsWithoutFieldPath(t *testing.T) {
	indexed := groupEventsByContainer([]*v1.Event{schedulerScheduledEvent(baselineWithOffset(4)), nil})

	assert.Empty(t, indexed)
}

func TestEventsInTimeOrder(t *testing.T) {
	events := []*v1.Event{
		podEvent("spec.containers{main}", "Started", "", baselineWithOffset(30)),
		nil,
		{Reason: "Scheduled", EventTime: metav1.NewMicroTime(baselineWithOffset(2).Time)},
		podEvent("spec.containers{main}", "Created", "", baselineWithOffset(20)),
	}

	ordered := eventsInTimeOrder(events)

	assert.Equal(t, []string{"Scheduled", "Created", "Started"},
		[]string{ordered[0].Reason, ordered[1].Reason, ordered[2].Reason})
	assert.Len(t, ordered, 3, "nil events are dropped")
}
