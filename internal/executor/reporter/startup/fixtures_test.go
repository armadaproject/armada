package startup

import (
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/executor/configuration"
)

// baseTime is the instant every fixture is relative to. A function rather than a package variable,
// so each caller builds its own value and nothing is shared between tests.
func baseTime() time.Time {
	return time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)
}

// baselineWithOffset is a point on the fixture timeline: baseTime plus a number of seconds. It
// produces a time, which is what every helper below takes - none of them accept offsets.
func baselineWithOffset(offsetSeconds int) metav1.Time {
	return metav1.NewTime(baseTime().Add(time.Duration(offsetSeconds) * time.Second))
}

func mustTimestamp(t *testing.T, ts *types.Timestamp) time.Time {
	t.Helper()
	require.NotNil(t, ts)
	parsed, err := types.TimestampFromProto(ts)
	require.NoError(t, err)
	return parsed.UTC()
}

func mustDuration(t *testing.T, d *types.Duration) time.Duration {
	t.Helper()
	require.NotNil(t, d)
	parsed, err := types.DurationFromProto(d)
	require.NoError(t, err)
	return parsed
}

func newTestTimings(t *testing.T) *Timings {
	t.Helper()
	timings, err := NewTimings(configuration.StartUpTimingsConfig{})
	require.NoError(t, err)
	return timings
}

// podWithConditions is a pod whose whole start-up is described by its conditions: scheduled at +2s,
// acknowledged by the kubelet at +3s, sandbox ready at +9s.
func podWithConditions() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-pod",
			CreationTimestamp: baselineWithOffset(0),
		},
		Status: v1.PodStatus{
			StartTime: new(baselineWithOffset(3)),
			Conditions: []v1.PodCondition{
				{Type: v1.PodScheduled, Status: v1.ConditionTrue, LastTransitionTime: baselineWithOffset(2)},
				{Type: v1.PodReadyToStartContainers, Status: v1.ConditionTrue, LastTransitionTime: baselineWithOffset(9)},
			},
		},
	}
}

func podEvent(fieldPath, reason, message string, firstTimestamp metav1.Time) *v1.Event {
	return &v1.Event{
		InvolvedObject: v1.ObjectReference{Kind: "Pod", Name: "test-pod", FieldPath: fieldPath},
		Reason:         reason,
		Message:        message,
		FirstTimestamp: firstTimestamp,
	}
}

// schedulerScheduledEvent mirrors what kube-scheduler actually records on bind. It deliberately
// leaves FirstTimestamp unset and sets EventTime instead: the scheduler uses the events.k8s.io
// recorder, which populates only EventTime, so in the core/v1 view FirstTimestamp is empty.
func schedulerScheduledEvent(eventTime metav1.Time) *v1.Event {
	return &v1.Event{
		InvolvedObject: v1.ObjectReference{Kind: "Pod", Name: "test-pod"},
		Source:         v1.EventSource{Component: "default-scheduler"},
		Reason:         "Scheduled",
		Message:        "Successfully assigned test-ns/test-pod to node-1",
		EventTime:      metav1.NewMicroTime(eventTime.Time),
	}
}

func terminated(startedAt, finishedAt metav1.Time) v1.ContainerState {
	return v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}}
}

func running(startedAt metav1.Time) v1.ContainerState {
	return v1.ContainerState{Running: &v1.ContainerStateRunning{StartedAt: startedAt}}
}

// startedUpPod is the pod from the design doc's worked example: scheduled at +2s, kubelet at +3s,
// sandbox at +9s, two init containers running 41s and 8s, app container started at +137s.
func startedUpPod() *v1.Pod {
	pod := podWithConditions()
	pod.Status.InitContainerStatuses = []v1.ContainerStatus{
		{Name: "fetch-dataset", State: terminated(baselineWithOffset(70), baselineWithOffset(111))},
		{Name: "warm-cache", State: terminated(baselineWithOffset(112), baselineWithOffset(120))},
	}
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{Name: "main", State: running(baselineWithOffset(137))},
	}
	return pod
}

func startedUpPodEvents() []*v1.Event {
	return []*v1.Event{
		// Every scheduled pod has this, and it is the source scheduled_at is read from.
		schedulerScheduledEvent(baselineWithOffset(1)),
		podEvent("spec.initContainers{fetch-dataset}", "Pulling", `Pulling image "repo/fetch:1.0"`, baselineWithOffset(9)),
		podEvent("spec.initContainers{fetch-dataset}", "Pulled",
			`Successfully pulled image "repo/fetch:1.0" in 58.204s (1m1.902s including waiting). Image size: 2415919104 bytes.`, baselineWithOffset(69)),
		podEvent("spec.initContainers{fetch-dataset}", "Created", "Created container: fetch-dataset", baselineWithOffset(69)),
		podEvent("spec.initContainers{fetch-dataset}", "Started", "Started container fetch-dataset", baselineWithOffset(70)),
		podEvent("spec.initContainers{warm-cache}", "Pulled", `Container image "repo/warm:1.0" already present on machine`, baselineWithOffset(111)),
		podEvent("spec.containers{main}", "Pulled",
			`Successfully pulled image "repo/main:1.0" in 9.881s (11.204s including waiting). Image size: 734003200 bytes.`, baselineWithOffset(134)),
		podEvent("spec.containers{main}", "Created", "Created container: main", baselineWithOffset(135)),
		podEvent("spec.containers{main}", "Started", "Started container main", baselineWithOffset(137)),
	}
}
