package startupmetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestRecord_PodMetrics(t *testing.T) {
	recorder := NewRecorder(configuration.StartUpMetricsConfig{})
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
	recorder := NewRecorder(configuration.StartUpMetricsConfig{})
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

// Every case is a pod with 50s of Kubernetes overhead: 10s before its init containers run and 40s
// after they finish, before the app container starts. The init containers run one after another, as
// they do on a real pod, so each one makes the pod start later.
func TestRecord_StartUpDurationVariants(t *testing.T) {
	type initContainer struct {
		name        string
		runDuration int
	}
	tracked := initContainer{name: "preflight", runDuration: 20}
	untracked := initContainer{name: "user-init", runDuration: 30}

	tests := map[string]struct {
		initContainers                         []initContainer
		expectedTotal                          float64
		expectedWithoutUntrackedInitContainers float64
		expectedWithoutInitContainers          float64
	}{
		"no init containers": {
			expectedTotal:                          50,
			expectedWithoutUntrackedInitContainers: 50,
			expectedWithoutInitContainers:          50,
		},
		"tracked init container only": {
			initContainers:                         []initContainer{tracked},
			expectedTotal:                          70,
			expectedWithoutUntrackedInitContainers: 70,
			expectedWithoutInitContainers:          50,
		},
		"untracked init container only": {
			initContainers:                         []initContainer{untracked},
			expectedTotal:                          80,
			expectedWithoutUntrackedInitContainers: 50,
			expectedWithoutInitContainers:          50,
		},
		"tracked and untracked init containers": {
			initContainers:                         []initContainer{tracked, untracked},
			expectedTotal:                          100,
			expectedWithoutUntrackedInitContainers: 70,
			expectedWithoutInitContainers:          50,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			recorder := NewRecorder(configuration.StartUpMetricsConfig{TrackedInitContainers: []string{tracked.name}})
			// The metrics are global, so each case records under its own labels.
			queue, pool, node := name+"-queue", name+"-pool", name+"-node"

			offset := 10
			var initContainers []*armadaevents.ContainerStartUpInfo
			for _, container := range tc.initContainers {
				initContainers = append(initContainers, &armadaevents.ContainerStartUpInfo{
					Name:       container.name,
					StartedAt:  baselineWithOffset(offset),
					FinishedAt: baselineWithOffset(offset + container.runDuration),
				})
				offset += container.runDuration
			}
			appStartedAt := baselineWithOffset(offset + 40)

			recorder.Record(testPod(queue, pool, node), &armadaevents.PodStartUpInfo{
				CreatedAt:      baselineWithOffset(0),
				StartedAt:      appStartedAt,
				InitContainers: initContainers,
				Containers:     []*armadaevents.ContainerStartUpInfo{{Name: "main", StartedAt: appStartedAt}},
			})

			_, sum := observations(t, podStartUpDurationSeconds, queue, pool)
			assert.Equal(t, tc.expectedTotal, sum)
			_, sum = observations(t, nodeStartUpDurationSeconds, node, pool)
			assert.Equal(t, tc.expectedTotal, sum)

			_, sum = observations(t, podStartUpWithoutUntrackedInitContainersDurationSeconds, queue, pool)
			assert.Equal(t, tc.expectedWithoutUntrackedInitContainers, sum)
			_, sum = observations(t, nodeStartUpWithoutUntrackedInitContainersDurationSeconds, node, pool)
			assert.Equal(t, tc.expectedWithoutUntrackedInitContainers, sum)

			_, sum = observations(t, podStartUpWithoutInitContainersDurationSeconds, queue, pool)
			assert.Equal(t, tc.expectedWithoutInitContainers, sum)
			_, sum = observations(t, nodeStartUpWithoutInitContainersDurationSeconds, node, pool)
			assert.Equal(t, tc.expectedWithoutInitContainers, sum)
		})
	}
}

// An init container is labelled by name only when the administrator configured that name, as a
// container's name otherwise comes from whatever the job asked for.
func TestRecord_InitContainerLabelledByNameOnlyWhenTracked(t *testing.T) {
	recorder := NewRecorder(configuration.StartUpMetricsConfig{
		TrackedInitContainers: []string{"fetch-dataset"},
	})
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
	recorder := NewRecorder(configuration.StartUpMetricsConfig{})
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
	recorder := NewRecorder(configuration.StartUpMetricsConfig{})
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
	recorder := NewRecorder(configuration.StartUpMetricsConfig{})
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
	recorder := NewRecorder(configuration.StartUpMetricsConfig{})
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

func TestRecord_TotalImagePullDuration(t *testing.T) {
	realPull := func(duration int) *armadaevents.ImagePullInfo {
		return &armadaevents.ImagePullInfo{
			Duration: seconds(duration),
		}
	}

	cachedPull := func() *armadaevents.ImagePullInfo {
		return &armadaevents.ImagePullInfo{Cached: true, SizeBytes: sizeBytes(1024)}
	}

	infoWithPulls := func(initPull, appPull *armadaevents.ImagePullInfo) *armadaevents.PodStartUpInfo {
		return &armadaevents.PodStartUpInfo{
			InitContainers: []*armadaevents.ContainerStartUpInfo{{Name: "init", ImagePull: initPull}},
			Containers:     []*armadaevents.ContainerStartUpInfo{{Name: "main", ImagePull: appPull}},
		}
	}

	tests := map[string]struct {
		info     *armadaevents.PodStartUpInfo
		expected *float64
	}{
		"every container's pull is summed": {
			info:     infoWithPulls(realPull(58), realPull(20)),
			expected: new(float64(78)),
		},
		"a cached container contributes nothing": {
			info:     infoWithPulls(realPull(58), cachedPull()),
			expected: new(float64(58)),
		},
		"zero for a pod that pulled nothing at all": {
			info:     infoWithPulls(cachedPull(), cachedPull()),
			expected: new(float64(0)),
		},
		"skipped when a container's pull is undescribed": {
			info: infoWithPulls(realPull(58), nil),
		},
		"skipped when invalid pull details provided": {
			info: infoWithPulls(realPull(58), &armadaevents.ImagePullInfo{}),
		},
		"skipped when the pod has no containers at all": {
			info: &armadaevents.PodStartUpInfo{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			queue := "total-pull-" + name
			pool := "total-pull-pool"

			NewRecorder(configuration.StartUpMetricsConfig{}).Record(testPod(queue, pool, "node-1"), tc.info)

			count, sum := observations(t, podTotalImagePullDurationSeconds, queue, pool)
			if tc.expected == nil {
				assert.Zero(t, count)
				return
			}
			assert.Equal(t, uint64(1), count)
			assert.Equal(t, *tc.expected, sum)
		})
	}
}

func TestRecord_NoOps(t *testing.T) {
	var nilRecorder *Recorder
	enabledRecorder := NewRecorder(configuration.StartUpMetricsConfig{})

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
	NewRecorder(configuration.StartUpMetricsConfig{}).Record(nil, fullyMeasuredInfo())

	assert.Equal(t, float64(0), testutil.ToFloat64(podStartUpInfoCollectedTotal.WithLabelValues("", "true")))
}
