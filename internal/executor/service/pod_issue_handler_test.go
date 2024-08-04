package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clock "k8s.io/utils/clock/testing"

	commonutil "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	podchecksConfig "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/internal/executor/context"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestPodIssueService_DoesNothingIfNoPodsAreFound(t *testing.T) {
	podIssueService, _, _, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)

	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents, 0)
}

func TestPodIssueService_DoesNothingIfNoStuckPodsAreFound(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)
	runningPod := makeRunningPod()
	addPod(t, fakeClusterContext, runningPod)

	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents, 0)
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Len(t, remainingActivePods, 1)
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfStuckAndUnretryable(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)
	unretryableStuckPod := makeUnretryableStuckPod()
	addPod(t, fakeClusterContext, unretryableStuckPod)
	addPodEvents(fakeClusterContext, unretryableStuckPod, []*v1.Event{{Message: "Image pull has failed", Type: "Warning"}})

	podIssueService.HandlePodIssues()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	assert.Len(t, eventsReporter.ReceivedEvents[0].Event.Events, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, failedEvent.JobRunErrors.Errors, 1)
	assert.Contains(t, failedEvent.JobRunErrors.Errors[0].GetPodError().Message, "unrecoverable problem")
	assert.Contains(t, failedEvent.JobRunErrors.Errors[0].GetPodError().DebugMessage, "Image pull has failed")
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfStuckTerminating(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)
	terminatingPod := makeTerminatingPod()
	addPod(t, fakeClusterContext, terminatingPod)

	podIssueService.HandlePodIssues()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	assert.Len(t, eventsReporter.ReceivedEvents[0].Event.Events, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, failedEvent.JobRunErrors.Errors, 1)
	assert.Contains(t, failedEvent.JobRunErrors.Errors[0].GetPodError().Message, "terminating")
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfExceedsActiveDeadline(t *testing.T) {
	startTime := time.Now().Add(-time.Minute * 10)

	tests := map[string]struct {
		expectIssueDetected bool
		pod                 *v1.Pod
	}{
		"PodPastDeadline": {
			expectIssueDetected: true,
			// Created 10 mins ago, 5 min deadline
			pod: makePodWithDeadline(startTime, 300, 0),
		},
		"PodPastDeadlineWithinTerminationGracePeriod": {
			expectIssueDetected: false,
			// Created 10 mins ago, 5 min deadline, 10 minute grace period
			pod: makePodWithDeadline(startTime, 300, 600),
		},
		"PodWithNoStartTime": {
			expectIssueDetected: false,
			// Created 10 mins ago, 5 min deadline, no start time
			pod: makePodWithDeadline(time.Time{}, 300, 0),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
			require.NoError(t, err)
			addPod(t, fakeClusterContext, tc.pod)

			podIssueService.HandlePodIssues()

			remainingActivePods := getActivePods(t, fakeClusterContext)

			if tc.expectIssueDetected {
				assert.Equal(t, []*v1.Pod{}, remainingActivePods)
				assert.Len(t, eventsReporter.ReceivedEvents, 1)
				assert.Len(t, eventsReporter.ReceivedEvents[0].Event.Events, 1)
				failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
				assert.True(t, ok)
				assert.Len(t, failedEvent.JobRunErrors.Errors, 1)
				assert.Contains(t, failedEvent.JobRunErrors.Errors[0].GetPodError().Message, "exceeded active deadline")
			} else {
				assert.Equal(t, []*v1.Pod{tc.pod}, remainingActivePods)
				assert.Len(t, eventsReporter.ReceivedEvents, 0)
			}
		})
	}
}

func TestPodIssueService_DeletesPodAndReportsLeaseReturned_IfRetryableStuckPod(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)
	retryableStuckPod := makeRetryableStuckPod()
	addPod(t, fakeClusterContext, retryableStuckPod)
	addPodEvents(fakeClusterContext, retryableStuckPod, []*v1.Event{{Message: "Some other message", Type: "Warning"}})

	podIssueService.HandlePodIssues()

	// Deletes pod
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)

	// Reset events
	eventsReporter.ReceivedEvents = []reporter.EventMessage{}
	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents[0].Event.Events, 1)
	returnedEvent, ok := eventsReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, returnedEvent.JobRunErrors.Errors, 1)
	assert.True(t, returnedEvent.JobRunErrors.Errors[0].GetPodLeaseReturned() != nil)
	assert.Contains(t, returnedEvent.JobRunErrors.Errors[0].GetPodLeaseReturned().DebugMessage, "Some other message")
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfRetryableStuckPodStartsUpAfterDeletionCalled(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)
	retryableStuckPod := makeRetryableStuckPod()
	addPod(t, fakeClusterContext, retryableStuckPod)

	podIssueService.HandlePodIssues()

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
	assert.Len(t, eventsReporter.ReceivedEvents[0].Event.Events, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, failedEvent.JobRunErrors.Errors, 1)
	assert.True(t, failedEvent.JobRunErrors.Errors[0].GetPodError() != nil)
}

func TestPodIssueService_ReportsFailed_IfDeletedExternally(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)
	runningPod := makeRunningPod()
	protoJobId, err := armadaevents.ProtoUuidFromUlidString(util.ExtractJobId(runningPod))
	require.NoError(t, err)
	fakeClusterContext.SimulateDeletionEvent(runningPod)

	podIssueService.HandlePodIssues()

	// Reports Failed
	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	assert.Len(t, eventsReporter.ReceivedEvents[0].Event.Events, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, failedEvent.JobRunErrors.Errors, 1)
	assert.True(t, failedEvent.JobRunErrors.Errors[0].GetPodError() != nil)
	assert.Equal(t, failedEvent.JobRunErrors.JobId, protoJobId)
}

func TestPodIssueService_ReportsFailed_IfPodOfActiveRunGoesMissing(t *testing.T) {
	baseTime := time.Now()
	fakeClock := clock.NewFakeClock(baseTime)
	jobId := commonutil.NewULID()
	protoJobId, err := armadaevents.ProtoUuidFromUlidString(jobId)
	require.NoError(t, err)

	podIssueService, _, _, eventsReporter, err := setupTestComponents([]*job.RunState{createRunState(jobId, uuid.New().String(), job.Active)})
	require.NoError(t, err)
	podIssueService.clock = fakeClock

	podIssueService.HandlePodIssues()
	// Nothing should happen, until the issue has been seen for a configured amount of time
	assert.Len(t, eventsReporter.ReceivedEvents, 0)

	fakeClock.SetTime(baseTime.Add(10 * time.Minute))
	podIssueService.HandlePodIssues()
	// Reports Failed
	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	assert.Len(t, eventsReporter.ReceivedEvents[0].Event.Events, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, failedEvent.JobRunErrors.Errors, 1)
	assert.True(t, failedEvent.JobRunErrors.Errors[0].GetPodError() != nil)
	assert.Equal(t, failedEvent.JobRunErrors.JobId, protoJobId)
}

func TestPodIssueService_DoesNothing_IfMissingPodOfActiveRunReturns(t *testing.T) {
	baseTime := time.Now()
	fakeClock := clock.NewFakeClock(baseTime)
	runningPod := makeRunningPod()
	runState := createRunState(util.ExtractJobId(runningPod), util.ExtractJobRunId(runningPod), job.Active)
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{runState})
	require.NoError(t, err)
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
	podIssueService, runStateStore, _, eventsReporter, err := setupTestComponents([]*job.RunState{createRunState("job-1", "run-1", job.SuccessfulSubmission)})
	require.NoError(t, err)
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
	runningPod := makeRunningPod()
	runState := createRunState(util.ExtractJobId(runningPod), util.ExtractJobRunId(runningPod), job.SuccessfulSubmission)
	podIssueService, runStateStore, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{runState})
	require.NoError(t, err)
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

func setupTestComponents(initialRunState []*job.RunState) (*IssueHandler, *job.JobRunStateStore, *fakecontext.SyncFakeClusterContext, *mocks.FakeEventReporter, error) {
	fakeClusterContext := fakecontext.NewSyncFakeClusterContext()
	eventReporter := mocks.NewFakeEventReporter()
	pendingPodChecker := makePodChecker()
	runStateStore := job.NewJobRunStateStoreWithInitialState(initialRunState)
	stateChecksConfig := configuration.StateChecksConfiguration{
		DeadlineForSubmittedPodConsideredMissing: time.Minute * 15,
		DeadlineForActivePodConsideredMissing:    time.Minute * 5,
	}

	podIssueHandler, err := NewIssueHandler(
		runStateStore,
		fakeClusterContext,
		eventReporter,
		stateChecksConfig,
		pendingPodChecker,
		time.Minute*3,
	)

	return podIssueHandler, runStateStore, fakeClusterContext, eventReporter, err
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

func getActivePods(t *testing.T, clusterContext context.ClusterContext) []*v1.Pod {
	t.Helper()
	remainingActivePods, err := clusterContext.GetActiveBatchPods()
	if err != nil {
		t.Error(err)
	}
	return remainingActivePods
}

func makePodWithDeadline(startTime time.Time, deadlineSeconds, gracePeriodSeconds int) *v1.Pod {
	pod := makeTestPod(v1.PodStatus{Phase: v1.PodRunning})
	activeDeadlineSeconds := int64(deadlineSeconds)
	pod.Spec.ActiveDeadlineSeconds = &activeDeadlineSeconds
	terminationGracePeriodSeconds := int64(gracePeriodSeconds)
	pod.Spec.TerminationGracePeriodSeconds = &terminationGracePeriodSeconds
	podStartTime := metav1.NewTime(startTime)
	pod.Status.StartTime = &podStartTime
	return pod
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
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				domain.JobId:    commonutil.NewULID(),
				domain.Queue:    "queue-id-1",
				domain.JobRunId: uuid.New().String(),
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
	return pod
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

func addPod(t *testing.T, fakeClusterContext context.ClusterContext, runningPod *v1.Pod) {
	t.Helper()
	_, err := fakeClusterContext.SubmitPod(runningPod, "owner-1", []string{})
	if err != nil {
		t.Error(err)
	}
}

func addPodEvents(fakeClusterContext *fakecontext.SyncFakeClusterContext, pod *v1.Pod, events []*v1.Event) {
	fakeClusterContext.Events[util.ExtractJobId(pod)] = events
}
