package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	commonutil "github.com/armadaproject/armada/internal/common/util"
	podchecksConfig "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/internal/executor/context"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	reporter_fake "github.com/armadaproject/armada/internal/executor/reporter/fake"
	"github.com/armadaproject/armada/internal/executor/service/fake"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
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

func TestJobManager_DeletesPodAndReportsTerminated_IfLeasePreventedOnRunningPod(t *testing.T) {
	fakeClusterContext, mockLeaseService, mockEventsReporter, jobManager := makejobManagerWithTestDoubles()

	runningPod := makeRunningPod()
	addPod(t, fakeClusterContext, runningPod)
	// Prevent renewal of lease for pod
	mockLeaseService.NonrenewableJobIds = []string{util.ExtractJobId(runningPod)}

	pods, err := fakeClusterContext.GetBatchPods()
	assert.NoError(t, err)
	assert.Len(t, pods, 1)

	jobManager.ManageJobLeases()

	_, ok := mockEventsReporter.ReceivedEvents[0].(*api.JobTerminatedEvent)
	assert.True(t, ok)

	pods, err = fakeClusterContext.GetBatchPods()
	assert.NoError(t, err)
	assert.Len(t, pods, 0)
}

func TestJobManager_DoesNothing_IfLeaseRenewalPreventOnFinishedPod(t *testing.T) {
	fakeClusterContext, mockLeaseService, mockEventsReporter, jobManager := makejobManagerWithTestDoubles()
	runningPod := makeSucceededPod()
	addPod(t, fakeClusterContext, runningPod)
	// Prevent renewal of lease for pod
	mockLeaseService.NonrenewableJobIds = []string{util.ExtractJobId(runningPod)}

	pods, err := fakeClusterContext.GetBatchPods()
	assert.NoError(t, err)
	assert.Len(t, pods, 1)

	jobManager.ManageJobLeases()

	assert.Len(t, mockEventsReporter.ReceivedEvents, 0)

	pods, err = fakeClusterContext.GetBatchPods()
	assert.NoError(t, err)
	assert.Len(t, pods, 1)
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

func TestJobManager_ReportsDoneAndFailed_IfDeletedExternally(t *testing.T) {
	fakeClusterContext, mockLeaseService, eventsReporter, jobManager := makejobManagerWithTestDoubles()
	runningPod := makeRunningPod()
	fakeClusterContext.SimulateDeletionEvent(runningPod)

	jobManager.ManageJobLeases()

	assert.Zero(t, mockLeaseService.ReturnLeaseCalls)
	mockLeaseService.AssertReportDoneCalledOnceWith(t, []string{util.ExtractJobId(runningPod)})

	failedEvent, ok := eventsReporter.ReceivedEvents[0].(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Equal(t, failedEvent.JobId, util.ExtractJobId(runningPod))
}

func getActivePods(t *testing.T, clusterContext context.ClusterContext) []*v1.Pod {
	t.Helper()
	remainingActivePods, err := clusterContext.GetActiveBatchPods()
	if err != nil {
		t.Error(err)
	}
	return remainingActivePods
}

func makeSucceededPod() *v1.Pod {
	return makeTestPod(v1.PodStatus{Phase: v1.PodSucceeded})
}

func makeRunningPod() *v1.Pod {
	return makeTestPod(v1.PodStatus{Phase: v1.PodRunning})
}

func makeTerminatingPod() *v1.Pod {
	pod := makeTestPod(v1.PodStatus{Phase: v1.PodRunning})
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
						Message: "Image pull has failed",
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
						Message: "Some other message",
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
			UID:               types.UID(commonutil.NewULID()),
		},
		Spec: v1.PodSpec{
			NodeName: "node1",
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

func makejobManagerWithTestDoubles() (*fakecontext.SyncFakeClusterContext, *fake.MockLeaseService, *reporter_fake.FakeEventReporter, *JobManager) {
	fakeClusterContext := fakecontext.NewSyncFakeClusterContext()
	mockLeaseService := fake.NewMockLeaseService()
	eventReporter := reporter_fake.NewFakeEventReporter()
	jobContext := job.NewClusterJobContext(fakeClusterContext, makePodChecker(), time.Minute*3, 1)

	jobManager := NewJobManager(
		fakeClusterContext,
		jobContext,
		eventReporter,
		mockLeaseService)

	return fakeClusterContext, mockLeaseService, eventReporter, jobManager
}

func makePodChecker() podchecks.PodChecker {
	var cfg podchecksConfig.Checks
	cfg.Events = []podchecksConfig.EventCheck{
		{Regexp: "Image pull has failed", Type: "Warning", GracePeriod: time.Nanosecond, Action: podchecksConfig.ActionFail},
		{Regexp: "Some other message", Type: "Warning", GracePeriod: time.Nanosecond, Action: podchecksConfig.ActionRetry},
	}
	cfg.ContainerStatuses = []podchecksConfig.ContainerStatusCheck{
		{State: podchecksConfig.ContainerStateWaiting, ReasonRegexp: "ImagePullBackOff", GracePeriod: time.Nanosecond, Action: podchecksConfig.ActionFail},
		{State: podchecksConfig.ContainerStateWaiting, ReasonRegexp: "Some reason", GracePeriod: time.Nanosecond, Action: podchecksConfig.ActionRetry},
	}

	checker, err := podchecks.NewPodChecks(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to make pod checker: %v", err))
	}

	return checker
}
