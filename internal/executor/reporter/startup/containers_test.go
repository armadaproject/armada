package startup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func podWithContainers(
	initSpecs []v1.Container,
	initStatuses []v1.ContainerStatus,
	appStatuses []v1.ContainerStatus,
) *v1.Pod {
	return &v1.Pod{
		Spec: v1.PodSpec{InitContainers: initSpecs},
		Status: v1.PodStatus{
			InitContainerStatuses: initStatuses,
			ContainerStatuses:     appStatuses,
		},
	}
}

func TestGetContainerStartUpDetails_EmptyWhenNoContainers(t *testing.T) {
	details := getContainerStartUpDetails(&v1.Pod{}, nil, nil)

	assert.Nil(t, details.initContainers)
	assert.Nil(t, details.containers)
}

// Statuses are reported by Kubernetes in spec order, so init containers come back in the sequence
// they ran rather than needing to be sorted.
func TestGetContainerStartUpDetails_KeepsStatusOrder(t *testing.T) {
	pod := podWithContainers(nil, []v1.ContainerStatus{
		{Name: "first", State: terminated(baselineWithOffset(10), baselineWithOffset(20))},
		{Name: "second", State: terminated(baselineWithOffset(21), baselineWithOffset(30))},
		{Name: "third", State: terminated(baselineWithOffset(31), baselineWithOffset(40))},
	}, nil)

	details := getContainerStartUpDetails(pod, nil, nil)

	require.Len(t, details.initContainers, 3)
	assert.Equal(t, []string{"first", "second", "third"}, []string{
		details.initContainers[0].Name,
		details.initContainers[1].Name,
		details.initContainers[2].Name,
	})
}

// podWithOneInitContainer is the subject of the two tests below: one init container that ran from
// +70s to +111s, described with and without the events that accompany it.
func podWithOneInitContainer() *v1.Pod {
	return podWithContainers(nil, []v1.ContainerStatus{
		{Name: "init", Image: cachedImageTag, ImageID: cachedImageDigest, State: terminated(baselineWithOffset(70), baselineWithOffset(111))},
	}, nil)
}

func TestGetContainerStartUpDetails_InstantsFromStatusAndEvents(t *testing.T) {
	events := []*v1.Event{
		podEvent("spec.initContainers{init}", "Created", "Created container: init", baselineWithOffset(69)),
		podEvent("spec.initContainers{init}", "Pulled",
			`Successfully pulled image "repo/img:1.0" in 1s (3s including waiting). Image size: 111 bytes.`, baselineWithOffset(60)),
	}

	details := getContainerStartUpDetails(podWithOneInitContainer(), nodeWithCachedImage(1234), events)

	require.Len(t, details.initContainers, 1)
	init := details.initContainers[0]
	assert.Equal(t, baselineWithOffset(69).Time, mustTimestamp(t, init.CreatedAt), "from the Created event")
	assert.Equal(t, baselineWithOffset(70).Time, mustTimestamp(t, init.StartedAt), "from terminated.startedAt")
	assert.Equal(t, baselineWithOffset(111).Time, mustTimestamp(t, init.FinishedAt), "from terminated.finishedAt")
	require.NotNil(t, init.ImagePull)
	assert.Equal(t, time.Second, mustDuration(t, init.ImagePull.Duration))
	assert.Equal(t, 2*time.Second, mustDuration(t, init.ImagePull.WaitingTime))
	require.NotNil(t, init.ImagePull.SizeBytes)
	assert.Equal(t, uint64(111), init.ImagePull.SizeBytes.Value, "the Pulled event's size wins over the node's")
}

// created_at and the image pull are only ever reported by events, so without them those two are
// unset while everything the container's own status carries is still reported.
func TestGetContainerStartUpDetails_UnsetWhenNoEvents(t *testing.T) {
	details := getContainerStartUpDetails(podWithOneInitContainer(), nodeWithCachedImage(1234), nil)

	require.Len(t, details.initContainers, 1)
	init := details.initContainers[0]
	assert.Nil(t, init.CreatedAt, "only an event reports this")
	assert.Nil(t, init.ImagePull, "only an event reports this, even when the node knows the image's size")
	assert.Equal(t, baselineWithOffset(70).Time, mustTimestamp(t, init.StartedAt), "status still supplies this")
	assert.Equal(t, baselineWithOffset(111).Time, mustTimestamp(t, init.FinishedAt))
}

// An app container on a pod that has reached Running has not finished, so there is no instant to
// report for it.
func TestGetContainerStartUpDetails_NoFinishedAtForRunningContainer(t *testing.T) {
	pod := podWithContainers(nil, nil, []v1.ContainerStatus{
		{Name: "main", State: running(baselineWithOffset(137))},
	})

	details := getContainerStartUpDetails(pod, nil, nil)

	require.Len(t, details.containers, 1)
	assert.Equal(t, baselineWithOffset(137).Time, mustTimestamp(t, details.containers[0].StartedAt))
	assert.Nil(t, details.containers[0].FinishedAt)
}

// The kubelet attributes an event through a field path that differs for init and app containers, so
// two containers sharing a name do not pick up each other's events.
func TestGetContainerStartUpDetails_SeparatesInitAndAppByFieldPath(t *testing.T) {
	pod := podWithContainers(nil,
		[]v1.ContainerStatus{{Name: "shared", State: terminated(baselineWithOffset(10), baselineWithOffset(20))}},
		[]v1.ContainerStatus{{Name: "shared", State: running(baselineWithOffset(30))}},
	)
	events := []*v1.Event{
		podEvent("spec.initContainers{shared}", "Created", "", baselineWithOffset(5)),
		podEvent("spec.containers{shared}", "Created", "", baselineWithOffset(25)),
	}

	details := getContainerStartUpDetails(pod, nil, events)

	require.Len(t, details.initContainers, 1)
	require.Len(t, details.containers, 1)
	assert.Equal(t, baselineWithOffset(5).Time, mustTimestamp(t, details.initContainers[0].CreatedAt))
	assert.Equal(t, baselineWithOffset(25).Time, mustTimestamp(t, details.containers[0].CreatedAt))
}

// A native sidecar is an init container declared with restartPolicy Always.
func TestGetContainerStartUpDetails_FlagsSidecarsFromSpec(t *testing.T) {
	pod := podWithContainers(
		[]v1.Container{
			{Name: "regular"},
			{Name: "logging-sidecar", RestartPolicy: new(v1.ContainerRestartPolicyAlways)},
			{Name: "explicitly-not", RestartPolicy: new(v1.ContainerRestartPolicy("OnFailure"))},
		},
		[]v1.ContainerStatus{
			{Name: "regular", State: terminated(baselineWithOffset(10), baselineWithOffset(20))},
			// Terminated rather than running, so this also covers a sidecar between restarts.
			{Name: "logging-sidecar", State: terminated(baselineWithOffset(21), baselineWithOffset(25))},
			{Name: "explicitly-not", State: terminated(baselineWithOffset(26), baselineWithOffset(30))},
		},
		[]v1.ContainerStatus{{Name: "main", State: running(baselineWithOffset(31))}})
	// An app container cannot be a sidecar even when it declares the same restart policy.
	pod.Spec.Containers = []v1.Container{{Name: "main", RestartPolicy: new(v1.ContainerRestartPolicyAlways)}}

	details := getContainerStartUpDetails(pod, nil, nil)

	require.Len(t, details.initContainers, 3)
	require.Len(t, details.containers, 1)
	assert.False(t, details.initContainers[0].Sidecar)
	assert.True(t, details.initContainers[1].Sidecar)
	assert.Equal(t, baselineWithOffset(25).Time, mustTimestamp(t, details.initContainers[1].FinishedAt),
		"a terminated sidecar still reports when it finished; the flag is what says not to sum it")
	assert.False(t, details.initContainers[2].Sidecar, "only Always makes an init container a sidecar")
	assert.False(t, details.containers[0].Sidecar, "an app container is never a sidecar")
}

func TestFirstKnown(t *testing.T) {
	first := baselineWithOffset(10).Time
	second := baselineWithOffset(20).Time

	assert.Equal(t, first, *firstKnown(&first, &second))
	assert.Equal(t, second, *firstKnown(nil, &second), "skips unknowns to reach the fallback")
	assert.Nil(t, firstKnown(nil, nil))
	assert.Nil(t, firstKnown())
}

func TestIsSidecarInitContainer(t *testing.T) {
	always := v1.ContainerRestartPolicyAlways
	pod := &v1.Pod{Spec: v1.PodSpec{
		InitContainers: []v1.Container{
			{Name: "regular"},
			{Name: "side", RestartPolicy: &always},
			{Name: "other-policy", RestartPolicy: new(v1.ContainerRestartPolicy("OnFailure"))},
		},
		// An app container is not an init container, so its restart policy is irrelevant here.
		Containers: []v1.Container{{Name: "main", RestartPolicy: &always}},
	}}

	assert.True(t, isSidecarInitContainer(pod, "side"))
	assert.False(t, isSidecarInitContainer(pod, "regular"))
	assert.False(t, isSidecarInitContainer(pod, "other-policy"))
	assert.False(t, isSidecarInitContainer(pod, "main"), "an app container is never a sidecar")
	assert.False(t, isSidecarInitContainer(pod, "not-in-the-spec"))
}

// The container's own status is the better source for its start - it is a pod field, so unlike the
// The event is the fallback for a status that reports no start time at all.
func TestGetContainerStartUpDetails_StartedAtSource(t *testing.T) {
	tests := map[string]struct {
		state    v1.ContainerState
		expected metav1.Time
	}{
		"status wins over the event": {
			state:    terminated(baselineWithOffset(70), baselineWithOffset(111)),
			expected: baselineWithOffset(70),
		},
		"falls back to the event": {
			state:    v1.ContainerState{Terminated: &v1.ContainerStateTerminated{FinishedAt: baselineWithOffset(111)}},
			expected: baselineWithOffset(99),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := podWithContainers(nil, []v1.ContainerStatus{{Name: "init", State: tc.state}}, nil)
			events := []*v1.Event{
				podEvent("spec.initContainers{init}", "Started", "Started container init", baselineWithOffset(99)),
			}

			details := getContainerStartUpDetails(pod, nil, events)

			require.Len(t, details.initContainers, 1)
			assert.Equal(t, tc.expected.Time, mustTimestamp(t, details.initContainers[0].StartedAt))
		})
	}
}
