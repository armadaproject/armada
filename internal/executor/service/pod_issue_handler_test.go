package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	reporter_fake "github.com/armadaproject/armada/internal/executor/reporter/fake"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
)

func TestPodIssueService_DoesNothingIfNoPodsAreFound(t *testing.T) {
	podIssueService, _, eventsReporter := setupTestComponents()

	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents, 0)
}

func TestPodIssueService_DoesNothingIfNoStuckPodsAreFound(t *testing.T) {
	podIssueService, fakeClusterContext, eventsReporter := setupTestComponents()
	runningPod := makeRunningPod()
	addPod(t, fakeClusterContext, runningPod)

	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents, 0)
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Len(t, remainingActivePods, 1)
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfStuckAndUnretryable(t *testing.T) {
	podIssueService, fakeClusterContext, eventsReporter := setupTestComponents()
	unretryableStuckPod := makeUnretryableStuckPod()
	addPod(t, fakeClusterContext, unretryableStuckPod)

	podIssueService.HandlePodIssues()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	assert.Len(t, eventsReporter.ReceivedEvents, 2)
	_, ok := eventsReporter.ReceivedEvents[0].Event.(*api.JobUnableToScheduleEvent)
	assert.True(t, ok)

	failedEvent, ok := eventsReporter.ReceivedEvents[1].Event.(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Contains(t, failedEvent.Reason, "unrecoverable problem")
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfStuckTerminating(t *testing.T) {
	podIssueService, fakeClusterContext, eventsReporter := setupTestComponents()
	terminatingPod := makeTerminatingPod()
	addPod(t, fakeClusterContext, terminatingPod)

	podIssueService.HandlePodIssues()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Contains(t, failedEvent.Reason, "terminating")
}

func TestPodIssueService_DeletesPodAndReportsLeaseReturned_IfRetryableStuckPod(t *testing.T) {
	podIssueService, fakeClusterContext, eventsReporter := setupTestComponents()
	retryableStuckPod := makeRetryableStuckPod()
	addPod(t, fakeClusterContext, retryableStuckPod)

	podIssueService.HandlePodIssues()

	// Deletes pod
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	// Reports UnableToSchedule and LeaseReturned
	assert.Len(t, eventsReporter.ReceivedEvents, 2)
	_, ok := eventsReporter.ReceivedEvents[0].Event.(*api.JobUnableToScheduleEvent)
	assert.True(t, ok)

	_, ok = eventsReporter.ReceivedEvents[1].Event.(*api.JobLeaseReturnedEvent)
	assert.True(t, ok)
}

func TestPodIssueService_ReportsFailed_IfDeletedExternally(t *testing.T) {
	podIssueService, fakeClusterContext, eventsReporter := setupTestComponents()
	runningPod := makeRunningPod()
	fakeClusterContext.SimulateDeletionEvent(runningPod)

	podIssueService.HandlePodIssues()

	// Reports Failed
	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Equal(t, failedEvent.JobId, util.ExtractJobId(runningPod))
}

func setupTestComponents() (*PodIssueService, *fakecontext.SyncFakeClusterContext, *reporter_fake.FakeEventReporter) {
	fakeClusterContext := fakecontext.NewSyncFakeClusterContext()
	eventReporter := reporter_fake.NewFakeEventReporter()
	pendingPodChecker := makePodChecker()

	podIssueHandler := NewPodIssueService(
		fakeClusterContext,
		eventReporter,
		pendingPodChecker,
		time.Minute*3,
	)

	return podIssueHandler, fakeClusterContext, eventReporter
}
