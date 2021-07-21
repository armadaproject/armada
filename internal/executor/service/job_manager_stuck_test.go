package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/service/fake"

	reporter_fake "github.com/G-Research/armada/internal/executor/reporter/fake"
	"github.com/G-Research/armada/pkg/api"
)

func TestJobManager_DoesNothingIfNoPodsAreFound(t *testing.T) {
	_, mockLeaseService, _, jobManager := makejobManagerWithTestDoubles()

	jobManager.ManageJobLeases()

	assert.Zero(t, mockLeaseService.ReturnLeaseCalls)

	mockLeaseService.AssertReportDoneCalledOnceWith(t, []string{})
}

func TestJobManager_DoesNothingIfNoStuckPodsAreFound(t *testing.T) {
	runningPod := makeRunningPod()

	fakeClusterContext, mockLeaseService, _, jobManager := makejobManagerWithTestDoubles()

	addPod(t, fakeClusterContext, runningPod)

	jobManager.ManageJobLeases()

	assert.Zero(t, mockLeaseService.ReturnLeaseCalls)

	mockLeaseService.AssertReportDoneCalledOnceWith(t, []string{})
}

func TestJobManager_DeletesPodAndReportsDoneIfStuckAndUnretryable(t *testing.T) {
	unretryableStuckPod := makeUnretryableStuckPod()

	fakeClusterContext, mockLeaseService, eventsReporter, jobManager := makejobManagerWithTestDoubles()

	addPod(t, fakeClusterContext, unretryableStuckPod)

	jobManager.ManageJobLeases()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	assert.Zero(t, mockLeaseService.ReturnLeaseCalls)

	mockLeaseService.AssertReportDoneCalledOnceWith(t, []string{unretryableStuckPod.Labels[domain.JobId]})

	_, ok := eventsReporter.ReceivedEvents[0].(*api.JobUnableToScheduleEvent)
	assert.True(t, ok)

	jobManager.ManageJobLeases()

	failedEvent, ok := eventsReporter.ReceivedEvents[1].(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Contains(t, failedEvent.Reason, "unrecoverable problem")
}

func TestJobManager_DeletesPodAndReportsFailedIfStuckTerminating(t *testing.T) {
	terminatingPod := makeTerminatingPod()

	fakeClusterContext, mockLeaseService, eventsReporter, jobManager := makejobManagerWithTestDoubles()

	addPod(t, fakeClusterContext, terminatingPod)

	jobManager.ManageJobLeases()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	assert.Zero(t, mockLeaseService.ReturnLeaseCalls)
	mockLeaseService.AssertReportDoneCalledOnceWith(t, []string{terminatingPod.Labels[domain.JobId]})

	jobManager.ManageJobLeases()

	failedEvent, ok := eventsReporter.ReceivedEvents[0].(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Contains(t, failedEvent.Reason, "terminating")
}

func TestJobManager_ReturnsLeaseAndDeletesRetryableStuckPod(t *testing.T) {
	retryableStuckPod := makeRetryableStuckPod()

	fakeClusterContext, mockLeaseService, _, jobManager := makejobManagerWithTestDoubles()

	addPod(t, fakeClusterContext, retryableStuckPod)

	jobManager.ManageJobLeases()

	// Not done as can be retried
	assert.Equal(t, 1, mockLeaseService.ReportDoneCalls)
	assert.Equal(t, []string{}, mockLeaseService.ReportDoneArg)

	// Not returning lease yet
	assert.Equal(t, 0, mockLeaseService.ReturnLeaseCalls)

	// Still deletes pod
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	jobManager.ManageJobLeases()

	// Not done as can be retried
	assert.Equal(t, 2, mockLeaseService.ReportDoneCalls)
	assert.Equal(t, []string{}, mockLeaseService.ReportDoneArg)

	// Return lease for retry
	assert.Equal(t, 1, mockLeaseService.ReturnLeaseCalls)
	assert.Equal(t, retryableStuckPod, mockLeaseService.ReturnLeaseArg)
}

func getActivePods(t *testing.T, clusterContext context.ClusterContext) []*v1.Pod {
	t.Helper()
	remainingActivePods, err := clusterContext.GetActiveBatchPods()
	if err != nil {
		t.Error(err)
	}
	return remainingActivePods
}

func makeRunningPod() *v1.Pod {
	return makeTestPod(v1.PodStatus{Phase: "Running"})
}

func makeTerminatingPod() *v1.Pod {
	pod := makeTestPod(v1.PodStatus{Phase: "Running"})
	t := metav1.NewTime(time.Now().Add(-time.Hour))
	pod.DeletionTimestamp = &t
	return pod
}

func makeUnretryableStuckPod() *v1.Pod {
	return makeTestPod(v1.PodStatus{
		Phase: "Pending",
		ContainerStatuses: []v1.ContainerStatus{
			{
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "ImagePullBackOff",
						Message: "Some message",
					},
				},
			},
		},
	})
}

func makeRetryableStuckPod() *v1.Pod {
	return makeTestPod(v1.PodStatus{
		Phase: "Pending",
		ContainerStatuses: []v1.ContainerStatus{
			{
				State: v1.ContainerState{
					Waiting: &v1.ContainerStateWaiting{
						Reason:  "Some reason",
						Message: "Some message",
					},
				},
			},
		},
	})
}

func makeTestPod(status v1.PodStatus) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				domain.JobId: "job-id-1",
				domain.Queue: "queue-id-1",
			},
			Annotations: map[string]string{
				domain.JobSetId: "job-set-id-1",
			},
			CreationTimestamp: metav1.Time{time.Now().Add(-10 * time.Minute)},
			UID:               types.UID(util.NewULID()),
		},
		Status: status,
	}
}

func addPod(t *testing.T, fakeClusterContext context.ClusterContext, runningPod *v1.Pod) {
	t.Helper()
	_, err := fakeClusterContext.SubmitPod(runningPod, "owner-1", []string{})
	if err != nil {
		t.Error(err)
	}
}

func makejobManagerWithTestDoubles() (context.ClusterContext, *fake.MockLeaseService, *reporter_fake.FakeEventReporter, *JobManager) {
	fakeClusterContext := fake.NewSyncFakeClusterContext()
	mockLeaseService := fake.NewMockLeaseService()
	eventReporter := reporter_fake.NewFakeEventReporter()
	jobContext := job.NewClusterJobContext(fakeClusterContext, time.Minute*3)

	jobManager := NewJobManager(
		fakeClusterContext,
		jobContext,
		eventReporter,
		mockLeaseService,
		time.Second,
		time.Second)

	return fakeClusterContext, mockLeaseService, eventReporter, jobManager
}
