package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/executor/configuration"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
)

func TestPodIssueService_DoesNothingIfNoPodsAreFound(t *testing.T) {
	podIssueService, _, _, eventsReporter := setupTestComponents([]*job.RunState{})

	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents, 0)
}

func TestPodIssueService_DoesNothingIfNoStuckPodsAreFound(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter := setupTestComponents([]*job.RunState{})
	runningPod := makeRunningPod(false)
	addPod(t, fakeClusterContext, runningPod)

	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents, 0)
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Len(t, remainingActivePods, 1)
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfStuckAndUnretryable(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter := setupTestComponents([]*job.RunState{})
	unretryableStuckPod := makeUnretryableStuckPod(false)
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
	podIssueService, _, fakeClusterContext, eventsReporter := setupTestComponents([]*job.RunState{})
	terminatingPod := makeTerminatingPod(false)
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
	podIssueService, _, fakeClusterContext, eventsReporter := setupTestComponents([]*job.RunState{})
	retryableStuckPod := makeRetryableStuckPod(false)
	addPod(t, fakeClusterContext, retryableStuckPod)

	podIssueService.HandlePodIssues()

	// Deletes pod
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	// Reports UnableToSchedule
	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	_, ok := eventsReporter.ReceivedEvents[0].Event.(*api.JobUnableToScheduleEvent)
	assert.True(t, ok)

	// Reset events
	eventsReporter.ReceivedEvents = []reporter.EventMessage{}
	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	_, ok = eventsReporter.ReceivedEvents[0].Event.(*api.JobLeaseReturnedEvent)
	assert.True(t, ok)
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfRetryableStuckPodStartsUpAfterDeletionCalled(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter := setupTestComponents([]*job.RunState{})
	retryableStuckPod := makeRetryableStuckPod(false)
	addPod(t, fakeClusterContext, retryableStuckPod)

	podIssueService.HandlePodIssues()

	// Reports UnableToSchedule
	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	_, ok := eventsReporter.ReceivedEvents[0].Event.(*api.JobUnableToScheduleEvent)
	assert.True(t, ok)

	// Reset events, and add pod back as running
	eventsReporter.ReceivedEvents = []reporter.EventMessage{}
	retryableStuckPod.Status.Phase = v1.PodRunning
	addPod(t, fakeClusterContext, retryableStuckPod)

	// Detects pod is now unexpectedly running and marks it non-retryable
	podIssueService.HandlePodIssues()
	assert.Len(t, eventsReporter.ReceivedEvents, 0)
	assert.Len(t, getActivePods(t, fakeClusterContext), 1)

	// Now processes the issue as non-retryable and fails the pod
	podIssueService.HandlePodIssues()
	assert.Len(t, getActivePods(t, fakeClusterContext), 0)

	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	_, ok = eventsReporter.ReceivedEvents[0].Event.(*api.JobFailedEvent)
	assert.True(t, ok)
}

func TestPodIssueService_ReportsFailed_IfDeletedExternally(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter := setupTestComponents([]*job.RunState{})
	runningPod := makeRunningPod(false)
	fakeClusterContext.SimulateDeletionEvent(runningPod)

	podIssueService.HandlePodIssues()

	// Reports Failed
	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Equal(t, failedEvent.JobId, util.ExtractJobId(runningPod))
}

func TestPodIssueService_ReportsFailed_IfPodOfActiveRunGoesMissing(t *testing.T) {
	baseTime := time.Now()
	fakeClock := clock.NewFakeClock(baseTime)
	podIssueService, _, _, eventsReporter := setupTestComponents([]*job.RunState{createRunState("job-1", "run-1", job.Active)})
	podIssueService.clock = fakeClock

	podIssueService.HandlePodIssues()
	// Nothing should happen, until the issue has been seen for a configured amount of time
	assert.Len(t, eventsReporter.ReceivedEvents, 0)

	fakeClock.SetTime(baseTime.Add(10 * time.Minute))
	podIssueService.HandlePodIssues()
	// Reports Failed
	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Equal(t, failedEvent.JobId, "job-1")
}

func TestPodIssueService_DoesNothing_IfMissingPodOfActiveRunReturns(t *testing.T) {
	baseTime := time.Now()
	fakeClock := clock.NewFakeClock(baseTime)
	runningPod := makeRunningPod(false)
	runState := createRunState(util.ExtractJobId(runningPod), util.ExtractJobRunId(runningPod), job.Active)
	podIssueService, _, fakeClusterContext, eventsReporter := setupTestComponents([]*job.RunState{runState})
	podIssueService.clock = fakeClock

	podIssueService.HandlePodIssues()
	// Nothing should happen, until the issue has been seen for a configured amount of time
	assert.Len(t, eventsReporter.ReceivedEvents, 0)

	addPod(t, fakeClusterContext, runningPod)
	fakeClock.SetTime(baseTime.Add(10 * time.Minute))
	podIssueService.HandlePodIssues()
	assert.Len(t, eventsReporter.ReceivedEvents, 0)
}

func TestPodIssueService_DeleteRunFromRunState_IfSubmittedPodNeverAppears(t *testing.T) {
	baseTime := time.Now()
	fakeClock := clock.NewFakeClock(baseTime)
	podIssueService, runStateStore, _, eventsReporter := setupTestComponents([]*job.RunState{createRunState("job-1", "run-1", job.SuccessfulSubmission)})
	podIssueService.clock = fakeClock

	podIssueService.HandlePodIssues()
	// Nothing should happen, until the issue has been seen for a configured amount of time
	assert.Len(t, eventsReporter.ReceivedEvents, 0)
	assert.Len(t, runStateStore.GetAll(), 1)

	fakeClock.SetTime(baseTime.Add(20 * time.Minute))
	podIssueService.HandlePodIssues()
	assert.Len(t, eventsReporter.ReceivedEvents, 0)
	// Pod has been missing for greater than configured period, run should get deleted
	assert.Len(t, runStateStore.GetAll(), 0)
}

func TestPodIssueService_DoesNothing_IfSubmittedPodAppears(t *testing.T) {
	baseTime := time.Now()
	fakeClock := clock.NewFakeClock(baseTime)
	runningPod := makeRunningPod(false)
	runState := createRunState(util.ExtractJobId(runningPod), util.ExtractJobRunId(runningPod), job.SuccessfulSubmission)
	podIssueService, runStateStore, fakeClusterContext, eventsReporter := setupTestComponents([]*job.RunState{runState})
	podIssueService.clock = fakeClock

	podIssueService.HandlePodIssues()
	// Nothing should happen, until the issue has been seen for a configured amount of time
	assert.Len(t, eventsReporter.ReceivedEvents, 0)
	assert.Len(t, runStateStore.GetAll(), 1)

	addPod(t, fakeClusterContext, runningPod)
	fakeClock.SetTime(baseTime.Add(20 * time.Minute))
	podIssueService.HandlePodIssues()
	assert.Len(t, runStateStore.GetAll(), 1)
}

func setupTestComponents(initialRunState []*job.RunState) (*IssueHandler, *job.JobRunStateStore, *fakecontext.SyncFakeClusterContext, *mocks.FakeEventReporter) {
	fakeClusterContext := fakecontext.NewSyncFakeClusterContext()
	eventReporter := mocks.NewFakeEventReporter()
	pendingPodChecker := makePodChecker()
	runStateStore := job.NewJobRunStateStoreWithInitialState(initialRunState)
	stateChecksConfig := configuration.StateChecksConfiguration{
		DeadlineForSubmittedPodConsideredMissing: time.Minute * 15,
		DeadlineForActivePodConsideredMissing:    time.Minute * 5,
	}

	podIssueHandler := NewIssueHandler(
		runStateStore,
		fakeClusterContext,
		eventReporter,
		stateChecksConfig,
		pendingPodChecker,
		time.Minute*3,
	)

	return podIssueHandler, runStateStore, fakeClusterContext, eventReporter
}

func createRunState(jobId string, runId string, phase job.RunPhase) *job.RunState {
	return &job.RunState{
		Phase: phase,
		Meta: &job.RunMeta{
			JobId: jobId,
			RunId: runId,
		},
	}
}
