package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/executor/configuration"
	context2 "github.com/G-Research/armada/internal/executor/fake/context"
	"github.com/G-Research/armada/internal/executor/job"
	reporter_fake "github.com/G-Research/armada/internal/executor/reporter/fake"
	"github.com/G-Research/armada/internal/executor/service/fake"
)

func TestCanBeRemovedConditions(t *testing.T) {
	s := createManager(time.Second, time.Second)
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
		result := s.canBeRemoved(&job.RunningJob{JobId: "", Pods: []*v1.Pod{pod}})
		assert.Equal(t, expected, result)
	}
}

func TestCanBeRemovedMinumumPodTime(t *testing.T) {
	s := createManager(5*time.Minute, 10*time.Minute)
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
		result := s.canBeRemoved(&job.RunningJob{JobId: "", Pods: []*v1.Pod{pod}})
		assert.Equal(t, expected, result)
	}
}

func makeFinishedPodWithTimestamp(state v1.PodPhase, timestamp time.Time) *v1.Pod {
	pod := makePodWithCurrentStateReported(state, true)
	pod.CreationTimestamp.Time = timestamp
	return pod
}

func makePodWithCurrentStateReported(state v1.PodPhase, reportedDone bool) *v1.Pod {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{string(state): time.Now().String()},
			// all times check fallback to creation time if there is no info available
			CreationTimestamp: metav1.Time{time.Now().Add(-10 * time.Minute)},
		},
		Status: v1.PodStatus{
			Phase: state,
		},
	}

	if reportedDone {
		pod.Annotations[jobDoneAnnotation] = time.Now().String()
	}

	return &pod
}

func createManager(minimumPodAge, failedPodExpiry time.Duration) *JobManager {
	fakeClusterContext := context2.NewFakeClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}, nil)
	fakeEventReporter := &reporter_fake.FakeEventReporter{}
	jobContext := job.NewClusterJobContext(fakeClusterContext)

	jobLeaseService := fake.NewMockLeaseService()
	stuckPodDetector := NewPodProgressMonitorService(fakeClusterContext, fakeEventReporter, jobLeaseService, time.Second)

	return NewJobManager(
		fakeClusterContext,
		jobContext,
		fakeEventReporter,
		stuckPodDetector,
		jobLeaseService,
		minimumPodAge,
		failedPodExpiry)
}
