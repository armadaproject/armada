package startup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMeasure_NilTimingsMeasuresNothing(t *testing.T) {
	var disabled *Timings

	assert.Nil(t, disabled.Measure(podWithConditions(), nil, nil))
}

func TestMeasure_PodAnchors(t *testing.T) {
	info := newTestTimings(t).Measure(startedUpPod(), startedUpPodNode(), startedUpPodEvents())

	require.NotNil(t, info)
	assert.Equal(t, baseTime(), mustTimestamp(t, info.CreatedAt))
	assert.Equal(t, baselineWithOffset(1).Time, mustTimestamp(t, info.ScheduledAt), "from the Scheduled event")
	assert.Equal(t, baselineWithOffset(3).Time, mustTimestamp(t, info.KubeletAcknowledgedAt))
	assert.Equal(t, baselineWithOffset(9).Time, mustTimestamp(t, info.SandboxReadyAt))
	assert.Equal(t, baselineWithOffset(137).Time, mustTimestamp(t, info.StartedAt))
}

func TestMeasure_InitContainers(t *testing.T) {
	info := newTestTimings(t).Measure(startedUpPod(), startedUpPodNode(), startedUpPodEvents())

	require.NotNil(t, info)
	require.Len(t, info.InitContainers, 2)

	fetch := info.InitContainers[0]
	assert.Equal(t, "fetch-dataset", fetch.Name)
	assert.Equal(t, baselineWithOffset(69).Time, mustTimestamp(t, fetch.CreatedAt))
	assert.Equal(t, baselineWithOffset(70).Time, mustTimestamp(t, fetch.StartedAt))
	assert.Equal(t, baselineWithOffset(111).Time, mustTimestamp(t, fetch.FinishedAt))
	assert.False(t, fetch.Sidecar)
	require.NotNil(t, fetch.ImagePull)
	assert.Equal(t, 58204*time.Millisecond, mustDuration(t, fetch.ImagePull.Duration))
	assert.Equal(t, 3698*time.Millisecond, mustDuration(t, fetch.ImagePull.WaitingTime))
	require.NotNil(t, fetch.ImagePull.SizeBytes)
	assert.Equal(t, uint64(2415919104), fetch.ImagePull.SizeBytes.Value, "the Pulled event's size wins over the node's")
	assert.False(t, fetch.ImagePull.Cached)

	warm := info.InitContainers[1]
	assert.Equal(t, "warm-cache", warm.Name)
	assert.Equal(t, baselineWithOffset(112).Time, mustTimestamp(t, warm.StartedAt))
	assert.Equal(t, baselineWithOffset(120).Time, mustTimestamp(t, warm.FinishedAt))
	require.NotNil(t, warm.ImagePull)
	assert.True(t, warm.ImagePull.Cached)
	// A cache hit has no pull to time, so both are left unset rather than reported as zero.
	assert.Nil(t, warm.ImagePull.Duration)
	assert.Nil(t, warm.ImagePull.WaitingTime)
	// The event carries no size for a cache hit, so it comes from the node instead.
	require.NotNil(t, warm.ImagePull.SizeBytes)
	assert.Equal(t, uint64(268435456), warm.ImagePull.SizeBytes.Value)
}

func TestMeasure_AppContainers(t *testing.T) {
	info := newTestTimings(t).Measure(startedUpPod(), startedUpPodNode(), startedUpPodEvents())

	require.NotNil(t, info)
	require.Len(t, info.Containers, 1)

	main := info.Containers[0]
	assert.Equal(t, "main", main.Name)
	assert.Equal(t, baselineWithOffset(135).Time, mustTimestamp(t, main.CreatedAt))
	assert.Equal(t, baselineWithOffset(137).Time, mustTimestamp(t, main.StartedAt))
	assert.Nil(t, main.FinishedAt, "an app container that is still running has not finished")
	assert.False(t, main.Sidecar, "an app container is never a sidecar")
	require.NotNil(t, main.ImagePull)
	assert.Equal(t, 9881*time.Millisecond, mustDuration(t, main.ImagePull.Duration))
	assert.Equal(t, 1323*time.Millisecond, mustDuration(t, main.ImagePull.WaitingTime))
	require.NotNil(t, main.ImagePull.SizeBytes)
	assert.Equal(t, uint64(734003200), main.ImagePull.SizeBytes.Value)
	assert.False(t, main.ImagePull.Cached)
}

func TestMeasure_ReturnsNilWhenNothingDeterminable(t *testing.T) {
	bare := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", CreationTimestamp: baselineWithOffset(0)}}

	assert.Nil(t, newTestTimings(t).Measure(bare, nil, nil))
}
