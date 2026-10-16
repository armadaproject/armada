package startup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
)

func TestGetPodCreationTime(t *testing.T) {
	pod := podWithConditions()

	assert.Equal(t, baseTime(), *GetPodCreationTime(pod))
}

func TestGetPodScheduledTime(t *testing.T) {
	scheduledEvent := []*v1.Event{schedulerScheduledEvent(baselineWithOffset(1))}
	unrelatedEvent := []*v1.Event{podEvent("spec.containers{main}", "Pulled", "something else", baselineWithOffset(1))}

	tests := map[string]struct {
		conditions []v1.PodCondition
		events     []*v1.Event
		expected   *time.Time
	}{
		"from the event": {
			events:   scheduledEvent,
			expected: new(baselineWithOffset(1).Time),
		},
		// The condition reports the kubelet picking the pod up at +2s, which is later than the bind
		// and is not what this field means.
		"event wins over the condition": {
			conditions: podWithConditions().Status.Conditions,
			events:     scheduledEvent,
			expected:   new(baselineWithOffset(1).Time),
		},
		"falls back to the condition when no event survives": {
			conditions: podWithConditions().Status.Conditions,
			expected:   new(baselineWithOffset(2).Time),
		},
		"falls back when only unrelated events are seen": {
			conditions: podWithConditions().Status.Conditions,
			events:     unrelatedEvent,
			expected:   new(baselineWithOffset(2).Time),
		},
		// The kubelet can report a condition with no transition time at all, which tells us nothing.
		"nil when the condition has no timestamp and no event survives": {
			conditions: []v1.PodCondition{{Type: v1.PodScheduled, Status: v1.ConditionTrue}},
		},
		"nil when neither is available": {},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := podWithConditions()
			pod.Status.Conditions = tc.conditions

			actual := GetPodScheduledTime(pod, tc.events)

			if tc.expected == nil {
				assert.Nil(t, actual)
				return
			}
			require.NotNil(t, actual)
			assert.Equal(t, *tc.expected, *actual)
		})
	}
}

func TestGetKubeletAcknowledgedTime(t *testing.T) {
	assert.Equal(t, baselineWithOffset(3).Time, *GetKubeletAcknowledgedTime(podWithConditions()))
}

func TestGetKubeletAcknowledgedTime_NilWhenStartTimeUnset(t *testing.T) {
	pod := podWithConditions()
	pod.Status.StartTime = nil

	assert.Nil(t, GetKubeletAcknowledgedTime(pod))
}

func TestGetSandboxReadyTime(t *testing.T) {
	assert.Equal(t, baselineWithOffset(9).Time, *GetSandboxReadyTime(podWithConditions()))
}

func TestGetSandboxReadyTime_NilWhenFeatureGateOff(t *testing.T) {
	pod := podWithConditions()
	pod.Status.Conditions = []v1.PodCondition{
		{Type: v1.PodScheduled, Status: v1.ConditionTrue, LastTransitionTime: baselineWithOffset(2)},
	}

	assert.Nil(t, GetSandboxReadyTime(pod))
}

func TestGetLatestAppContainerStartTime(t *testing.T) {
	tests := map[string]struct {
		statuses []v1.ContainerStatus
		expected *time.Time
	}{
		"no containers": {},
		"none started": {
			statuses: []v1.ContainerStatus{
				{Name: "waiting", State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "ContainerCreating"}}},
			},
		},
		"latest of several wins regardless of order": {
			statuses: []v1.ContainerStatus{
				{Name: "late", State: running(baselineWithOffset(45))},
				{Name: "early", State: running(baselineWithOffset(20))},
			},
			expected: new(baselineWithOffset(45).Time),
		},
		// The phase flips when the last app container first starts, so one that started late and has
		// since crashed must still set the end of start up - otherwise a pod that took 200s to come
		// up reports the healthy container's 20s and looks fast.
		"a container that has since crashed still counts": {
			statuses: []v1.ContainerStatus{
				{Name: "healthy", State: running(baselineWithOffset(20))},
				{
					Name:                 "crash-looping",
					State:                v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
					LastTerminationState: terminated(baselineWithOffset(200), baselineWithOffset(205)),
				},
			},
			expected: new(baselineWithOffset(200).Time),
		},
		"containers not yet started are skipped": {
			statuses: []v1.ContainerStatus{
				{Name: "started", State: running(baselineWithOffset(20))},
				{Name: "waiting", State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "ContainerCreating"}}},
			},
			expected: new(baselineWithOffset(20).Time),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := &v1.Pod{Status: v1.PodStatus{ContainerStatuses: tc.statuses}}

			actual := GetLatestAppContainerStartTime(pod)

			if tc.expected == nil {
				assert.Nil(t, actual)
				return
			}
			require.NotNil(t, actual)
			assert.Equal(t, *tc.expected, *actual)
		})
	}
}

// The earliest start the status still reports, from whichever state carries it. A container that
// has restarted describes its current incarnation in State and the previous one in LastState, and
// the earlier of the two belongs on the pod's timeline: what the pod waited for was the container
// first starting, not the restart.
func TestGetContainerStartedTime(t *testing.T) {
	tests := map[string]struct {
		state     v1.ContainerState
		lastState v1.ContainerState
		expected  *time.Time
	}{
		"terminated":                 {state: terminated(baselineWithOffset(70), baselineWithOffset(111)), expected: new(baselineWithOffset(70).Time)},
		"running":                    {state: running(baselineWithOffset(10)), expected: new(baselineWithOffset(10).Time)},
		"waiting":                    {state: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "ContainerCreating"}}},
		"terminated without a start": {state: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{FinishedAt: baselineWithOffset(20)}}},
		// A container that restarted describes its current incarnation in State and the previous one
		// in LastState. The earlier belongs on the timeline: the pod waited for the first start.
		"restarted, earlier incarnation wins": {
			state:     running(baselineWithOffset(210)),
			lastState: terminated(baselineWithOffset(200), baselineWithOffset(205)),
			expected:  new(baselineWithOffset(200).Time),
		},
		"waiting to restart, lastState is the only record it ran": {
			state:     v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			lastState: terminated(baselineWithOffset(200), baselineWithOffset(205)),
			expected:  new(baselineWithOffset(200).Time),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actual := GetContainerStartedTime(v1.ContainerStatus{Name: "c", State: tc.state, LastTerminationState: tc.lastState})
			if tc.expected == nil {
				assert.Nil(t, actual)
				return
			}
			require.NotNil(t, actual)
			assert.Equal(t, *tc.expected, *actual)
		})
	}
}

func TestGetContainerFinishedTime(t *testing.T) {
	assert.Equal(t, baselineWithOffset(111).Time, *GetContainerFinishedTime(v1.ContainerStatus{State: terminated(baselineWithOffset(70), baselineWithOffset(111))}))
}

func TestGetContainerFinishedTime_NilWhenNotFinished(t *testing.T) {
	tests := map[string]v1.ContainerState{
		"still running":  {Running: &v1.ContainerStateRunning{StartedAt: baselineWithOffset(10)}},
		"waiting":        {Waiting: &v1.ContainerStateWaiting{Reason: "ContainerCreating"}},
		"no finish time": {Terminated: &v1.ContainerStateTerminated{StartedAt: baselineWithOffset(10)}},
	}

	for name, state := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Nil(t, GetContainerFinishedTime(v1.ContainerStatus{Name: "c", State: state}))
		})
	}
}

// A condition's lastTransitionTime is when its status last changed - in either direction. Reading it
// without checking the status reports the moment a condition went False as though it had been met,
// which is what a completed or degraded pod looks like: PodReadyToStartContainers goes False when
// the sandbox is torn down, and ContainersReady goes False when a container crash-loops.
func TestPodConditionAnchors_NilWhenConditionIsNotTrue(t *testing.T) {
	for _, status := range []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionUnknown} {
		t.Run(string(status), func(t *testing.T) {
			pod := podWithConditions()
			pod.Status.Conditions = []v1.PodCondition{
				{Type: v1.PodScheduled, Status: status, LastTransitionTime: baselineWithOffset(2)},
				{Type: v1.PodReadyToStartContainers, Status: status, LastTransitionTime: baselineWithOffset(9)},
			}

			assert.Nil(t, GetPodScheduledTime(pod, nil))
			assert.Nil(t, GetSandboxReadyTime(pod))
		})
	}
}

// A container that has restarted reports its current incarnation in state and the previous one in
// lastState. The earlier of those is what belongs on the pod's timeline: the pod waited for the
// container's first start, not for the restart.
func TestToTimestamp(t *testing.T) {
	instant := baselineWithOffset(10).Time

	converted := toTimestamp(&instant)

	require.NotNil(t, converted)
	assert.Equal(t, instant, mustTimestamp(t, converted))
}

func TestToTimestamp_NilWhenUnknown(t *testing.T) {
	assert.Nil(t, toTimestamp(nil))
}

// Sub-second precision survives the round trip, which matters for the one anchor that has it: a
// scheduler event's EventTime is a MicroTime.
func TestToTimestamp_PreservesSubSecondPrecision(t *testing.T) {
	precise := baseTime().Add(1500 * time.Millisecond)

	assert.Equal(t, precise, mustTimestamp(t, toTimestamp(&precise)))
}
