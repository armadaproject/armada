package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
)

func TestCleanUpResources_RemovesExpiredResources(t *testing.T) {
	s := createResourceCleanupService(time.Second, time.Second, 10)
	now := time.Now()

	succeededExpiredPod := makeFinishedPodWithTimestamp(v1.PodSucceeded, now.Add(-1*time.Minute))
	failedExpiredPod := makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-1*time.Minute))
	addPods(t, s.clusterContext, succeededExpiredPod, failedExpiredPod)

	s.CleanupResources()

	remainingPods, err := s.clusterContext.GetBatchPods()
	assert.NoError(t, err)
	assert.Empty(t, remainingPods)
}

func TestCleanUpResources_LeavesNonExpiredPods(t *testing.T) {
	s := createResourceCleanupService(time.Minute*5, time.Minute*5, 10)
	now := time.Now()

	succeededNonExpiredPod := makeFinishedPodWithTimestamp(v1.PodSucceeded, now.Add(-1*time.Minute))
	failedNonExpiredPod := makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-1*time.Minute))
	runningPod := makePodWithCurrentStateReported(v1.PodRunning, false)
	pendingPod := makePodWithCurrentStateReported(v1.PodPending, false)
	addPods(t, s.clusterContext, succeededNonExpiredPod, failedNonExpiredPod, runningPod, pendingPod)

	s.CleanupResources()

	remainingPods, err := s.clusterContext.GetBatchPods()
	assert.NoError(t, err)
	assert.Len(t, remainingPods, 4)
}

func TestCleanUpResources_RemovesNonExpiredPodsOverMaxTerminatedPodLimit(t *testing.T) {
	s := createResourceCleanupService(time.Minute*5, time.Minute*5, 1)
	now := time.Now()

	succeededNonExpiredPod := makeFinishedPodWithTimestamp(v1.PodSucceeded, now.Add(-1*time.Minute))
	failedNonExpiredPod := makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-2*time.Minute))
	addPods(t, s.clusterContext, succeededNonExpiredPod, failedNonExpiredPod)

	s.CleanupResources()

	remainingPods, err := s.clusterContext.GetBatchPods()
	assert.NoError(t, err)
	assert.Len(t, remainingPods, 1)
	assert.Equal(t, remainingPods[0].Name, succeededNonExpiredPod.Name)
}

func TestCanBeRemovedConditions(t *testing.T) {
	s := createResourceCleanupService(time.Second, time.Second, 1)
	pods := map[*v1.Pod]bool{
		// should not be cleaned yet
		makePodWithCurrentStateReported(v1.PodRunning, false):   false,
		makePodWithCurrentStateReported(v1.PodSucceeded, false): false,
		makePodWithCurrentStateReported(v1.PodFailed, false):    false,

		// should be cleaned
		makePodWithCurrentStateReported(v1.PodSucceeded, true): true,
		makePodWithCurrentStateReported(v1.PodFailed, true):    true,
	}

	for pod, expected := range pods {
		result := s.canPodBeRemoved(pod)
		assert.Equal(t, expected, result)
	}
}

func TestCanBeRemovedMinimumPodTime(t *testing.T) {
	s := createResourceCleanupService(5*time.Minute, 10*time.Minute, 1)
	now := time.Now()
	pods := map[*v1.Pod]bool{
		// should not be cleaned yet
		makeFinishedPodWithTimestamp(v1.PodSucceeded, now.Add(-1*time.Minute)): false,
		makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-1*time.Minute)):    false,
		makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-7*time.Minute)):    false,

		// should be cleaned
		makeFinishedPodWithTimestamp(v1.PodSucceeded, now.Add(-7*time.Minute)): true,
		makeFinishedPodWithTimestamp(v1.PodFailed, now.Add(-13*time.Minute)):   true,
	}

	for pod, expected := range pods {
		result := s.canPodBeRemoved(pod)
		assert.Equal(t, expected, result)
	}
}

func TestGetOldestPodsWithQueueFairShare(t *testing.T) {
	now := time.Now()

	queue1Pod1 := makePodWithFinishedTimestamp("queueA", now.Add(-2*time.Minute))
	queue1Pod2 := makePodWithFinishedTimestamp("queueA", now.Add(-6*time.Minute))
	queue1Pod3 := makePodWithFinishedTimestamp("queueA", now.Add(-9*time.Minute))

	queue2Pod1 := makePodWithFinishedTimestamp("queueB", now.Add(-8*time.Minute))
	queue2Pod2 := makePodWithFinishedTimestamp("queueB", now.Add(-9*time.Minute))

	queue3Pod1 := makePodWithFinishedTimestamp("queueC", now.Add(-30*time.Minute))
	pods := []*v1.Pod{
		queue1Pod1,
		queue1Pod2,
		queue1Pod3,
		queue2Pod1,
		queue2Pod2,
		queue3Pod1,
	}

	// Gets oldest from largest queue
	oldestPods := getOldestPodsWithQueueFairShare(pods, 1)
	assert.Len(t, oldestPods, 1)
	assert.True(t, contains(oldestPods, queue1Pod3))

	// Gets oldest with share over queues
	oldestPods = getOldestPodsWithQueueFairShare(pods, 3)
	assert.Len(t, oldestPods, 3)
	assert.True(t, contains(oldestPods, queue1Pod3))
	assert.True(t, contains(oldestPods, queue1Pod2))
	assert.True(t, contains(oldestPods, queue2Pod2))
}

func TestGetOldestPodsWithQueueFairShare_HandlesTooHighPodLimit(t *testing.T) {
	now := time.Now()

	queue1Pod1 := makePodWithFinishedTimestamp("queueA", now.Add(-2*time.Minute))

	pods := []*v1.Pod{
		queue1Pod1,
	}

	// Gets oldest from largest queue
	oldestPods := getOldestPodsWithQueueFairShare(pods, 2)
	assert.Len(t, oldestPods, 1)
	assert.True(t, contains(oldestPods, queue1Pod1))
}

func TestGetOldestPodsWithQueueFairShare_ConsidersPodsWithoutTimingDataAsOldest(t *testing.T) {
	now := time.Now()

	var zero time.Time
	queue1Pod1 := makePodWithFinishedTimestamp("queueA", now.Add(-2*time.Minute))
	queue1Pod2 := makePodWithFinishedTimestamp("queueA", zero)

	pods := []*v1.Pod{
		queue1Pod1,
		queue1Pod2,
	}

	// Gets oldest from largest queue
	oldestPods := getOldestPodsWithQueueFairShare(pods, 1)
	assert.Len(t, oldestPods, 1)
	assert.True(t, contains(oldestPods, queue1Pod2))
}

func contains(pods []*v1.Pod, pod *v1.Pod) bool {
	podNames := util.ExtractNames(pods)
	podsSet := util2.StringListToSet(podNames)

	_, exists := podsSet[pod.Name]
	return exists
}

func makePodWithFinishedTimestamp(queue string, timestamp time.Time) *v1.Pod {
	pod := makePod(queue)
	pod.CreationTimestamp.Time = timestamp
	return pod
}

func makeFinishedPodWithTimestamp(state v1.PodPhase, timestamp time.Time) *v1.Pod {
	pod := makePodWithCurrentStateReported(state, true)
	// all times check fallback to creation time if there is no info available
	pod.CreationTimestamp.Time = timestamp
	return pod
}

func makePodWithCurrentStateReported(state v1.PodPhase, reportedDone bool) *v1.Pod {
	pod := makePod("")
	pod.ObjectMeta.Annotations[string(state)] = time.Now().String()
	pod.Status = v1.PodStatus{
		Phase: state,
	}

	if reportedDone {
		pod.Annotations[domain.JobDoneAnnotation] = time.Now().String()
	}

	return pod
}

func makePod(queue string) *v1.Pod {
	if queue == "" {
		queue = "default"
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        util2.NewULID(),
			UID:         types.UID(util2.NewULID()),
			Namespace:   "default",
			Annotations: map[string]string{},
			Labels:      map[string]string{domain.Queue: queue, domain.JobId: util2.NewULID()},
		},
	}
	return pod
}

func addPods(t *testing.T, clusterContext clusterContext.ClusterContext, pods ...*v1.Pod) {
	for _, pod := range pods {
		_, err := clusterContext.SubmitPod(pod, "Owner", []string{})
		assert.NoError(t, err)
	}
}

func createResourceCleanupService(minimumPodAge, failedPodExpiry time.Duration, maxTerminatedPods int) *ResourceCleanupService {
	fakeClusterContext := fake.NewSyncFakeClusterContext()
	kubernetesConfig := configuration.KubernetesConfiguration{
		MinimumPodAge:     minimumPodAge,
		FailedPodExpiry:   failedPodExpiry,
		MaxTerminatedPods: maxTerminatedPods,
	}

	return NewResourceCleanupService(
		fakeClusterContext,
		kubernetesConfig)
}
