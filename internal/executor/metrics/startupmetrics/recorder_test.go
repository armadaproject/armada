package startupmetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/executor/configuration"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// newTestRecorder has an empty cluster, as the clean up sweep is exercised on its own with a
// cluster of its own.
func newTestRecorder(trackedInitContainers ...string) *Recorder {
	return NewRecorder(
		configuration.StartUpMetricsConfig{TrackedInitContainers: trackedInitContainers},
		fakecontext.NewSyncFakeClusterContext(),
	)
}

func TestRecord_PodMetrics(t *testing.T) {
	recorder := newTestRecorder()
	queue, pool := "pod-metrics-queue", "pod-metrics-pool"

	recorder.Record(testPod(queue, pool, "node-1"), fullyMeasuredInfo())

	count, sum := observations(t, podStartUpDurationSeconds, queue, pool)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(137), sum)

	count, sum = observations(t, podStartUpStepDurationSeconds, queue, pool, schedulingStep)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(2), sum)

	count, sum = observations(t, podStartUpStepDurationSeconds, queue, pool, sandboxCreationStep)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(7), sum)

	// The init container ran from +70s to +111s, under the grouped label as it is not configured.
	count, sum = observations(t, podInitContainerRunDurationSeconds, queue, pool, untrackedContainer)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(41), sum)

	// Both images were described completely, the app container's from the node's cache.
	assert.Equal(t, float64(1), testutil.ToFloat64(podImagePullInfoTotal.WithLabelValues(queue, pool, "false", "true")))
	assert.Equal(t, float64(1), testutil.ToFloat64(podImagePullInfoTotal.WithLabelValues(queue, pool, "true", "true")))

	// Only a real pull carries a duration and a waiting time, so only it is observed.
	count, sum = observations(t, podImagePullDurationSeconds, queue, pool)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(58), sum)

	count, sum = observations(t, podImagePullSizeBytes, queue, pool, "false")
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(2415919104), sum)

	// The cached image's size was resolved too, and is told apart by the label.
	count, sum = observations(t, podImagePullSizeBytes, queue, pool, "true")
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(734003200), sum)

	count, sum = observations(t, podImagePullWaitingDurationSeconds, queue, pool)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(3), sum)
}

func TestRecord_NodeMetrics(t *testing.T) {
	recorder := newTestRecorder()
	pool, node := "node-metrics-pool", "node-metrics-node"

	recorder.Record(testPod("node-metrics-queue", pool, node), fullyMeasuredInfo())

	count, sum := observations(t, nodeStartUpDurationSeconds, node, pool)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(137), sum)

	count, sum = observations(t, nodeStartUpStepDurationSeconds, node, pool, sandboxCreationStep)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(7), sum)

	count, sum = observations(t, nodeInitContainerRunDurationSeconds, node, pool, untrackedContainer)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(41), sum)

	count, sum = observations(t, nodeImagePullWaitingDurationSeconds, node, pool)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(3), sum)
}

// An init container is labelled by name only when the administrator configured that name, as a
// container's name otherwise comes from whatever the job asked for.
func TestRecord_InitContainerLabelledByNameOnlyWhenTracked(t *testing.T) {
	recorder := newTestRecorder("fetch-dataset")
	queue, pool := "tracked-queue", "tracked-pool"

	info := fullyMeasuredInfo()
	info.InitContainers = []*armadaevents.ContainerStartUpInfo{
		{
			Name:       "fetch-dataset",
			StartedAt:  baselineWithOffset(70),
			FinishedAt: baselineWithOffset(111),
		},
		{
			Name:       "warm-cache",
			StartedAt:  baselineWithOffset(112),
			FinishedAt: baselineWithOffset(120),
		},
	}
	recorder.Record(testPod(queue, pool, "node-1"), info)

	count, sum := observations(t, podInitContainerRunDurationSeconds, queue, pool, "fetch-dataset")
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(41), sum)

	count, sum = observations(t, podInitContainerRunDurationSeconds, queue, pool, untrackedContainer)
	assert.Equal(t, uint64(1), count)
	assert.Equal(t, float64(8), sum)

	assert.Zero(t, observationCount(t, podInitContainerRunDurationSeconds, queue, pool, "warm-cache"))
}

// Only an init container that ran to completion has a run duration. A sidecar is still running when
// the pod reaches Running, and an app container with a finish time has restarted rather than
// completed, so its last run is not part of start up at all.
func TestRecord_OnlyFinishedInitContainersHaveRunDuration(t *testing.T) {
	recorder := newTestRecorder()
	queue, pool := "unfinished-queue", "unfinished-pool"

	info := fullyMeasuredInfo()
	info.InitContainers[0].Sidecar = true
	info.InitContainers[0].FinishedAt = nil
	info.Containers[0].StartedAt = baselineWithOffset(100)
	info.Containers[0].FinishedAt = baselineWithOffset(130)
	recorder.Record(testPod(queue, pool, "node-1"), info)

	assert.Zero(t, observationCount(t, podInitContainerRunDurationSeconds, queue, pool, untrackedContainer))
	assert.Zero(t, observationCount(t, podInitContainerRunDurationSeconds, queue, pool, "main"))
}

func TestRecord_CollectionCounters(t *testing.T) {
	recorder := newTestRecorder()
	pool := "counters-pool"

	// A complete breakdown counts as collected and complete, with nothing against any field.
	recorder.Record(testPod("counters-queue", pool, "node-1"), fullyMeasuredInfo())

	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues(pool, "true")))
	assert.Equal(t, float64(0), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues(pool, "false")))
	assert.Equal(t, float64(0), testutil.ToFloat64(podStartUpInfoCollectionErrorTotal.WithLabelValues(pool, fieldStartedAt)))

	// A second pod missing an instant counts as collected but incomplete, and once against the field.
	incomplete := fullyMeasuredInfo()
	incomplete.StartedAt = nil
	recorder.Record(testPod("counters-queue", pool, "node-1"), incomplete)

	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues(pool, "true")))
	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues(pool, "false")))
	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectionErrorTotal.WithLabelValues(pool, fieldStartedAt)))

	// started_at is what the total is measured to, so it is not observed without it.
	assert.Equal(t, uint64(1), observationCount(t, podStartUpDurationSeconds, "counters-queue", pool))
}

// One pod whose containers are each missing several things counts once per field, as the metric
// answers whether collection is working rather than how many containers it fell down on.
func TestRecord_CollectionErrorCountedOncePerFieldPerPod(t *testing.T) {
	recorder := newTestRecorder()
	pool := "combined-pool"

	info := fullyMeasuredInfo()
	info.InitContainers = []*armadaevents.ContainerStartUpInfo{
		{Name: "first", ImagePull: &armadaevents.ImagePullInfo{}},
		{Name: "second", ImagePull: &armadaevents.ImagePullInfo{}},
	}
	recorder.Record(testPod("combined-queue", pool, "node-1"), info)

	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectionErrorTotal.WithLabelValues(pool, fieldInitContainers)))
	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectionErrorTotal.WithLabelValues(pool, fieldImagePullDetails)))
}

func TestRecord_ImagePullInfoCountedPerImage(t *testing.T) {
	recorder := newTestRecorder()
	queue, pool := "image-info-queue", "image-info-pool"

	info := fullyMeasuredInfo()
	info.InitContainers = []*armadaevents.ContainerStartUpInfo{
		// A real pull with everything, a cached pull whose best-effort size was not resolved, and a
		// container with no pull described at all.
		{
			Name:      "pulled",
			CreatedAt: baselineWithOffset(69),
			StartedAt: baselineWithOffset(70),
			ImagePull: &armadaevents.ImagePullInfo{
				Duration:    seconds(58),
				WaitingTime: seconds(3),
				SizeBytes:   sizeBytes(2415919104),
			},
		},
		{
			Name:      "cached-without-size",
			CreatedAt: baselineWithOffset(71),
			StartedAt: baselineWithOffset(72),
			ImagePull: &armadaevents.ImagePullInfo{Cached: true},
		},
		{
			Name:      "undescribed",
			CreatedAt: baselineWithOffset(73),
			StartedAt: baselineWithOffset(74),
		},
	}
	recorder.Record(testPod(queue, pool, "node-1"), info)

	assert.Equal(t, float64(1), testutil.ToFloat64(podImagePullInfoTotal.WithLabelValues(queue, pool, "false", "true")))
	assert.Equal(t, float64(1), testutil.ToFloat64(podImagePullInfoTotal.WithLabelValues(queue, pool, "true", "false")))
	assert.Equal(t, float64(1), testutil.ToFloat64(podImagePullInfoTotal.WithLabelValues(queue, pool, cachedUnknown, "false")))
	// Plus the app container of the fixture, whose cached pull did have its size.
	assert.Equal(t, float64(1), testutil.ToFloat64(podImagePullInfoTotal.WithLabelValues(queue, pool, "true", "true")))

	// The two kinds of image failure are told apart by field, and the pod is incomplete either way.
	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectionErrorTotal.WithLabelValues(pool, fieldCachedImagePullSize)))
	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectionErrorTotal.WithLabelValues(pool, fieldImagePull)))
	assert.Equal(t, float64(1), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues(pool, "false")))
}

func TestRecord_NoOps(t *testing.T) {
	var nilRecorder *Recorder
	enabledRecorder := newTestRecorder()

	tests := map[string]struct {
		recorder *Recorder
		pool     string
		info     *armadaevents.PodStartUpInfo
	}{
		"metrics disabled, so no recorder": {recorder: nilRecorder, pool: "no-op-pool-1", info: fullyMeasuredInfo()},
		"nothing measurable about the pod": {recorder: enabledRecorder, pool: "no-op-pool-2"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.recorder.Record(testPod("no-op-queue", tc.pool, "no-op-node"), tc.info)

			assert.Equal(t, float64(0), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues(tc.pool, "true")))
			assert.Equal(t, float64(0), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues(tc.pool, "false")))
			assert.Zero(t, observationCount(t, podStartUpDurationSeconds, "no-op-queue", tc.pool))
		})
	}
}

// A breakdown with no pod to read labels from is dropped rather than recorded under empty labels.
func TestRecord_NoPod(t *testing.T) {
	newTestRecorder().Record(nil, fullyMeasuredInfo())

	assert.Equal(t, float64(0), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues("", "true")))
}

func TestCleanUpStaleNodeMetrics(t *testing.T) {
	// Recorded against in every case and always in the cluster, so that removing one node's series
	// is seen not to remove another's.
	const untouched = "clean-up-untouched-node"

	tests := map[string]struct {
		node string
		// Built from the node under test, as whether it is in the cluster is what decides its fate.
		cluster func(node string) clusterContext.ClusterContext
		kept    bool
	}{
		"removes series - when node leaves the cluster": {
			node:    "clean-up-departed-node",
			cluster: func(string) clusterContext.ClusterContext { return clusterWithNodes(untouched) },
			kept:    false,
		},
		"maintains series - when a node still in the cluster": {
			node:    "clean-up-live-node",
			cluster: func(node string) clusterContext.ClusterContext { return clusterWithNodes(untouched, node) },
			kept:    true,
		},
		"maintains series - when node cannot be listed": {
			node: "clean-up-unlistable-node",
			cluster: func(string) clusterContext.ClusterContext {
				return clusterWhoseNodeListingFails{fakecontext.NewSyncFakeClusterContext()}
			},
			kept: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pool := "clean-up-pool"
			recorder := NewRecorder(configuration.StartUpMetricsConfig{}, tc.cluster(tc.node))
			recorder.Record(testPod("clean-up-queue", pool, tc.node), fullyMeasuredInfo())
			recorder.Record(testPod("clean-up-queue", pool, untouched), fullyMeasuredInfo())
			assertNodeObservations(t, tc.node, pool, 1)

			recorder.CleanUpStaleNodeMetrics()

			if tc.kept {
				assert.Contains(t, recorder.getSeenNodes(), tc.node)
				assertNodeObservations(t, tc.node, pool, 1)
			} else {
				assert.NotContains(t, recorder.getSeenNodes(), tc.node)
				assertNodeObservations(t, tc.node, pool, 0)
			}

			assert.Contains(t, recorder.getSeenNodes(), untouched)
			assert.NotZero(t, observationCount(t, nodeStartUpDurationSeconds, untouched, pool))
		})
	}
}

// assertNodeObservations covers every metric keyed by node, so that the sweep acting on one of them
// is not taken for it having acted on all four.
func assertNodeObservations(t *testing.T, node, pool string, expected uint64) {
	t.Helper()
	assert.Equal(t, expected, observationCount(t, nodeStartUpDurationSeconds, node, pool))
	assert.Equal(t, expected, observationCount(t, nodeStartUpStepDurationSeconds, node, pool, schedulingStep))
	assert.Equal(t, expected, observationCount(t, nodeInitContainerRunDurationSeconds, node, pool, untrackedContainer))
	assert.Equal(t, expected, observationCount(t, nodeImagePullWaitingDurationSeconds, node, pool))
}
