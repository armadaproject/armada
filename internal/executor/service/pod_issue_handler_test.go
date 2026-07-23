package service

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	realclock "k8s.io/utils/clock"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/errormatch"
	commonutil "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/categorizer"
	"github.com/armadaproject/armada/internal/executor/configuration"
	podchecksConfig "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/internal/executor/context"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/podchecks/failedpodchecks"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const retryableFailedPodStatusMessage = "retryable"

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

func TestPodIssueService_DoesNothingToTerminalPods(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)
	unretryableFailedStuckPod := makeUnretryableStuckPod()
	unretryableFailedStuckPod.Status.Phase = v1.PodFailed
	addPod(t, fakeClusterContext, unretryableFailedStuckPod)
	addPodEvents(fakeClusterContext, unretryableFailedStuckPod, []*v1.Event{{Message: "Image pull has failed", Type: "Warning"}})

	unretryableSuccessfulStuckPod := makeUnretryableStuckPod()
	unretryableSuccessfulStuckPod.Status.Phase = v1.PodSucceeded
	addPod(t, fakeClusterContext, unretryableSuccessfulStuckPod)
	addPodEvents(fakeClusterContext, unretryableSuccessfulStuckPod, []*v1.Event{{Message: "Image pull has failed", Type: "Warning"}})

	podIssueService.HandlePodIssues()

	assert.Len(t, eventsReporter.ReceivedEvents, 0)
	assert.Len(t, podIssueService.knownPodIssues, 0)
	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Len(t, remainingActivePods, 2)
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
	assert.NotEqual(t, errormatch.CategoryInternal, failedEvent.JobRunErrors.Errors[0].GetFailureCategory())
}

func TestPodIssueService_StructuralIssueIsInternal_RegardlessOfClassifier(t *testing.T) {
	classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{
			{
				Name: "oom-failure",
				Rules: []categorizer.CategoryRule{
					{OnConditions: []string{"OOMKilled"}, Subcategory: "kernel-oom"},
				},
			},
		},
	})
	require.NoError(t, err)

	podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
	require.NoError(t, err)

	pod := makeTerminatingPod()
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{Name: "main", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}},
	}
	addPod(t, fakeClusterContext, pod)

	podIssueService.HandlePodIssues()

	require.Len(t, eventReporter.ReceivedEvents, 1)
	failedEvent, ok := eventReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	require.True(t, ok)
	assert.Equal(t, errormatch.CategoryInternal, failedEvent.JobRunErrors.Errors[0].GetFailureCategory())
	assert.Equal(t, errormatch.SubcategoryStuckTerminating, failedEvent.JobRunErrors.Errors[0].GetFailureSubcategory())
}

func TestPodIssueService_OnPodErrorClassifies(t *testing.T) {
	tests := map[string]struct {
		category              string
		subcategory           string
		pattern               string
		hint                  string
		pod                   func() *v1.Pod
		expectMessageContains string
	}{
		"platform mismatch from kubelet error with hint": {
			category:    "infrastructure",
			subcategory: "platform_mismatch",
			pattern:     "no match for platform in manifest",
			hint:        "Build for the cluster's CPU architecture (typically x64/arm64 mismatch)",
			pod: func() *v1.Pod {
				p := makeUnretryableStuckPod()
				p.Status.ContainerStatuses[0].State.Waiting.Message = `Failed to pull image "amd64/busybox:latest": no match for platform in manifest`
				return p
			},
			expectMessageContains: "no match for platform in manifest",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			classifier := podErrorClassifier(t, tc.category, tc.subcategory, tc.pattern, tc.hint)
			podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
			require.NoError(t, err)
			addPod(t, fakeClusterContext, tc.pod())

			podIssueService.HandlePodIssues()

			require.Len(t, eventReporter.ReceivedEvents, 1)
			failedEvent, ok := eventReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
			require.True(t, ok)

			assert.Equal(t, tc.category, failedEvent.JobRunErrors.Errors[0].GetFailureCategory())
			assert.Equal(t, tc.subcategory, failedEvent.JobRunErrors.Errors[0].GetFailureSubcategory())
			message := failedEvent.JobRunErrors.Errors[0].GetPodError().Message
			assert.Contains(t, message, tc.expectMessageContains)
			if tc.hint != "" {
				rawIdx := strings.Index(message, tc.expectMessageContains)
				hintIdx := strings.Index(message, tc.hint)
				require.GreaterOrEqual(t, rawIdx, 0, "raw error must appear in message")
				require.GreaterOrEqual(t, hintIdx, 0, "hint must appear in message")
				assert.Greater(t, hintIdx, rawIdx, "hint must come after raw error, not before; defends against prepend regression")
			}
		})
	}
}

func TestPodIssueService_OnlyDeletesPod_IfStuckTerminatingButDeletedByExecutor(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)
	terminatingPod := makeTerminatingPod()
	terminatingPod.Annotations[domain.MarkedForDeletion] = time.Now().String()
	addPod(t, fakeClusterContext, terminatingPod)

	podIssueService.HandlePodIssues()

	remainingActivePods := getActivePods(t, fakeClusterContext)
	assert.Equal(t, []*v1.Pod{}, remainingActivePods)
	assert.Len(t, eventsReporter.ReceivedEvents, 0)
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

func TestPodIssueService_HasIssue(t *testing.T) {
	podIssueService, _, _, _, err := setupTestComponents([]*job.RunState{})
	require.NoError(t, err)

	issue := &runIssue{
		JobId: "abc",
		RunId: "def",
	}

	added, err := podIssueService.registerIssue(issue)
	assert.True(t, added)
	assert.NoError(t, err)

	// Empty input
	result := podIssueService.HasIssue("")
	assert.False(t, result)

	// unknown id
	result = podIssueService.HasIssue("unknown")
	assert.False(t, result)

	// known id
	result = podIssueService.HasIssue(issue.RunId)
	assert.True(t, result)

	// after issue resolve
	podIssueService.markIssuesResolved(issue)
	result = podIssueService.HasIssue(issue.RunId)
	assert.False(t, result)
}

func TestPodIssueService_DetectAndRegisterFailedPodIssue(t *testing.T) {
	failedPodWithRetryableIssue := makeTestPod(v1.PodStatus{Phase: v1.PodFailed})
	failedPodWithRetryableIssue.Status.Message = retryableFailedPodStatusMessage

	failedPodWithNonRetryableIssue := makeTestPod(v1.PodStatus{Phase: v1.PodFailed})
	failedPodWithNonRetryableIssue.Status.Message = "non-retryable"
	tests := map[string]struct {
		pod                      *v1.Pod
		issueAlreadyExists       bool
		shouldErrorGettingEvents bool
		expectIssueAdded         bool
		expectError              bool
	}{
		"FailedPodWithIssue": {
			pod:              failedPodWithRetryableIssue,
			expectIssueAdded: true,
			expectError:      false,
		},
		"FailedPodWithoutIssue": {
			pod:              failedPodWithNonRetryableIssue,
			expectIssueAdded: false,
			expectError:      false,
		},
		"FailedPodWithIssue_IssueAlreadyRegistered": {
			pod:                failedPodWithRetryableIssue,
			issueAlreadyExists: true,
			expectIssueAdded:   false,
			expectError:        false,
		},
		"FailedPodWithIssue_EventErrors": {
			pod:                      failedPodWithRetryableIssue,
			shouldErrorGettingEvents: true,
			expectIssueAdded:         false,
			expectError:              true,
		},
		"UnmanagedPod": {
			pod:              &v1.Pod{},
			expectIssueAdded: false,
			expectError:      false,
		},
		"RunningPod": {
			pod:              makeRunningPod(),
			expectIssueAdded: false,
			expectError:      false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			podIssueService, _, fakeClusterContext, _, err := setupTestComponents([]*job.RunState{})
			require.NoError(t, err)
			if tc.issueAlreadyExists {
				added, err := podIssueService.registerIssue(&runIssue{
					JobId: util.ExtractJobId(tc.pod),
					RunId: util.ExtractJobRunId(tc.pod),
				})
				assert.True(t, added)
				assert.NoError(t, err)
			}
			if tc.shouldErrorGettingEvents {
				fakeClusterContext.GetPodEventsErr = fmt.Errorf("failed getting events")
			}

			issueAdded, err := podIssueService.DetectAndRegisterFailedPodIssue(tc.pod)

			assert.Equal(t, tc.expectIssueAdded, issueAdded)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPodIssueService_DeletesPodAndReportsFailed_IfExceedsActiveDeadline(t *testing.T) {
	startTime := time.Now().Add(-time.Minute * 10)

	completedPodPastDeadline := makePodWithDeadline(startTime, 300, 0)
	completedPodPastDeadline.Status.Phase = v1.PodFailed
	completedPodPastDeadline.Annotations[string(v1.PodFailed)] = "true"

	tests := map[string]struct {
		expectIssueDetected bool
		pod                 *v1.Pod
	}{
		"PodPastDeadline": {
			expectIssueDetected: true,
			// Created 10 mins ago, 5 min deadline
			pod: makePodWithDeadline(startTime, 300, 0),
		},
		"PodPastDeadline - Completed pod": {
			expectIssueDetected: false,
			pod:                 completedPodPastDeadline,
		},
		"PodPastDeadlineWithinTerminationGracePeriod": {
			expectIssueDetected: false,
			// Created 10 mins ago, 5 min deadline, 10 minute grace period
			pod: makePodWithDeadline(startTime, 300, 600),
		},
		"PodWithNoStartTime": {
			expectIssueDetected: false,
			// No start time so cannot determine if past its deadline
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
				assert.Equal(t, errormatch.CategoryInternal, failedEvent.JobRunErrors.Errors[0].GetFailureCategory())
				assert.Equal(t, errormatch.SubcategoryActiveDeadline, failedEvent.JobRunErrors.Errors[0].GetFailureSubcategory())
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
	jobId := util.ExtractJobId(runningPod)
	fakeClusterContext.SimulateDeletionEvent(runningPod)

	podIssueService.HandlePodIssues()

	// Reports Failed
	assert.Len(t, eventsReporter.ReceivedEvents, 1)
	assert.Len(t, eventsReporter.ReceivedEvents[0].Event.Events, 1)
	failedEvent, ok := eventsReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, failedEvent.JobRunErrors.Errors, 1)
	assert.True(t, failedEvent.JobRunErrors.Errors[0].GetPodError() != nil)
	assert.Equal(t, jobId, failedEvent.JobRunErrors.JobId)
	assert.Equal(t, errormatch.CategoryInternal, failedEvent.JobRunErrors.Errors[0].GetFailureCategory())
	assert.Equal(t, errormatch.SubcategoryExternallyDeleted, failedEvent.JobRunErrors.Errors[0].GetFailureSubcategory())
}

func TestPodIssueService_ReportsFailed_IfPodOfActiveRunGoesMissing(t *testing.T) {
	baseTime := time.Now()
	fakeClock := clock.NewFakeClock(baseTime)
	jobId := commonutil.NewULID()

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
	assert.Equal(t, jobId, failedEvent.JobRunErrors.JobId)
	assert.Equal(t, errormatch.CategoryInternal, failedEvent.JobRunErrors.Errors[0].GetFailureCategory())
	assert.Equal(t, errormatch.SubcategoryPodMissing, failedEvent.JobRunErrors.Errors[0].GetFailureSubcategory())
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

func setupTestComponents(initialRunState []*job.RunState) (*PodIssueHandler, *job.JobRunStateStore, *fakecontext.SyncFakeClusterContext, *mocks.FakeEventReporter, error) {
	return setupTestComponentsWithClassifier(initialRunState, nil)
}

func setupTestComponentsWithClassifier(initialRunState []*job.RunState, classifier *categorizer.Classifier) (*PodIssueHandler, *job.JobRunStateStore, *fakecontext.SyncFakeClusterContext, *mocks.FakeEventReporter, error) {
	fakeClusterContext := fakecontext.NewSyncFakeClusterContext()
	eventReporter := mocks.NewFakeEventReporter()
	pendingPodChecker := makePendingPodChecker()
	failedPodChecker := makeFailedPodChecker()
	runStateStore := job.NewJobRunStateStoreWithInitialState(initialRunState)
	stateChecksConfig := configuration.StateChecksConfiguration{
		DeadlineForSubmittedPodConsideredMissing: time.Minute * 15,
		DeadlineForActivePodConsideredMissing:    time.Minute * 5,
	}

	podIssueHandler, err := NewPodIssuerHandler(
		runStateStore,
		fakeClusterContext,
		eventReporter,
		stateChecksConfig,
		pendingPodChecker,
		failedPodChecker,
		time.Minute*3,
		classifier,
	)

	return podIssueHandler, runStateStore, fakeClusterContext, eventReporter, err
}

func conditionClassifier(t *testing.T, category, subcategory, condition string) *categorizer.Classifier {
	t.Helper()
	c, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{
			{
				Name: category,
				Rules: []categorizer.CategoryRule{
					{OnConditions: []string{condition}, Subcategory: subcategory},
				},
			},
		},
	})
	require.NoError(t, err)
	return c
}

func podErrorClassifier(t *testing.T, category, subcategory, pattern, hint string) *categorizer.Classifier {
	t.Helper()
	c, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{
			{
				Name: category,
				Rules: []categorizer.CategoryRule{
					{OnPodError: &errormatch.RegexMatcher{Pattern: pattern}, Subcategory: subcategory, Hint: hint},
				},
			},
		},
	})
	require.NoError(t, err)
	return c
}

// The metric counter itself is tested in the metrics package. These tests
// cover the emission gating for the non-retryable and retryable issue paths:
// only a successful Report call should have led to a counter increment.

func TestPodIssueService_EmitsFailedEventWhenClassifierMatches(t *testing.T) {
	classifier := conditionClassifier(t, "pih-success-cat", "pih-success-sub", errormatch.ConditionOOMKilled)
	podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
	require.NoError(t, err)

	pod := makeTerminatingPod()
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{Name: "main", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 137, Reason: errormatch.ConditionOOMKilled}}},
	}
	addPod(t, fakeClusterContext, pod)

	podIssueService.HandlePodIssues()

	assert.Len(t, eventReporter.ReceivedEvents, 1)
}

func TestPodIssueService_SuppressesEmissionWhenReportFails(t *testing.T) {
	classifier := conditionClassifier(t, "pih-report-error-cat", "pih-report-error-sub", errormatch.ConditionOOMKilled)
	podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
	require.NoError(t, err)
	eventReporter.ErrorOnReport = true

	pod := makeTerminatingPod()
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{Name: "main", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 137, Reason: errormatch.ConditionOOMKilled}}},
	}
	addPod(t, fakeClusterContext, pod)

	podIssueService.HandlePodIssues()

	assert.Len(t, eventReporter.ReceivedEvents, 0, "Report errored - event must not be recorded as delivered")
}

func TestPodIssueService_EmitsEventWithEmptyCategoryWhenClassifierIsNil(t *testing.T) {
	podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, nil)
	require.NoError(t, err)

	pod := makeTerminatingPod()
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{Name: "main", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 137, Reason: errormatch.ConditionOOMKilled}}},
	}
	addPod(t, fakeClusterContext, pod)

	podIssueService.HandlePodIssues()

	assert.Len(t, eventReporter.ReceivedEvents, 1, "event still emitted with empty category/subcategory when classification is disabled")
}

func TestPodIssueService_RetryableIssue_LeaseReturnClassification(t *testing.T) {
	hint := "Check the image reference and registry availability"
	tests := map[string]struct {
		classifier        *categorizer.Classifier
		expectCategory    string
		expectSubcategory string
		expectHint        string
	}{
		"matching rule sets category and records failure": {
			classifier:        podErrorClassifier(t, "pih-retry-cat", "pih-retry-sub", "Unable to start pod", hint),
			expectCategory:    "pih-retry-cat",
			expectSubcategory: "pih-retry-sub",
			expectHint:        hint,
		},
		"nil classifier leaves category empty": {},
		"no matching rule leaves category empty": {
			classifier: podErrorClassifier(t, "pih-retry-nomatch-cat", "pih-retry-nomatch-sub", "pattern that never matches", ""),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, tc.classifier)
			require.NoError(t, err)
			counterBefore := failureCounterValue(t, tc.expectCategory, tc.expectSubcategory)
			familyTotalBefore := failureCounterFamilyTotal(t)

			retryableStuckPod := makeRetryableStuckPod()
			addPod(t, fakeClusterContext, retryableStuckPod)

			// First pass deletes the pod, second pass emits the lease return.
			podIssueService.HandlePodIssues()
			podIssueService.HandlePodIssues()

			require.Len(t, eventReporter.ReceivedEvents, 1)
			returnedEvent, ok := eventReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
			require.True(t, ok)
			leaseReturnError := returnedEvent.JobRunErrors.Errors[0]
			require.NotNil(t, leaseReturnError.GetPodLeaseReturned())
			assert.Equal(t, tc.expectCategory, leaseReturnError.GetFailureCategory())
			assert.Equal(t, tc.expectSubcategory, leaseReturnError.GetFailureSubcategory())

			message := leaseReturnError.GetPodLeaseReturned().Message
			rawIdx := strings.Index(message, "Unable to start pod")
			require.GreaterOrEqual(t, rawIdx, 0, "raw error must appear in message")
			if tc.expectHint != "" {
				hintIdx := strings.Index(message, tc.expectHint)
				require.GreaterOrEqual(t, hintIdx, 0, "hint must appear in message")
				assert.Greater(t, hintIdx, rawIdx, "hint must come after raw error")
			}

			if tc.expectCategory != "" {
				assert.Equal(t, counterBefore+1, failureCounterValue(t, tc.expectCategory, tc.expectSubcategory))
			} else {
				assert.Equal(t, familyTotalBefore, failureCounterFamilyTotal(t), "unclassified lease return must not record a failure")
			}
		})
	}
}

func TestPodIssueService_RetryableIssue_NoRecordWhenReportFails(t *testing.T) {
	category, subcategory := "pih-retry-report-fail-cat", "pih-retry-report-fail-sub"
	classifier := podErrorClassifier(t, category, subcategory, "Unable to start pod", "")
	podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
	require.NoError(t, err)
	counterBefore := failureCounterValue(t, category, subcategory)

	retryableStuckPod := makeRetryableStuckPod()
	addPod(t, fakeClusterContext, retryableStuckPod)

	podIssueService.HandlePodIssues()
	eventReporter.ErrorOnReport = true
	podIssueService.HandlePodIssues()

	assert.Len(t, eventReporter.ReceivedEvents, 0)
	assert.Equal(t, counterBefore, failureCounterValue(t, category, subcategory), "failed sends must not increment the counter")

	// The issue stays registered, so a later pass reports and records.
	eventReporter.ErrorOnReport = false
	podIssueService.HandlePodIssues()

	assert.Len(t, eventReporter.ReceivedEvents, 1)
	assert.Equal(t, counterBefore+1, failureCounterValue(t, category, subcategory))
}

// failureCounterValue reads the executor job failure counter for the given
// label pair from the default prometheus registry, returning 0 when the
// labelled child does not exist yet.
func failureCounterValue(t *testing.T, category string, subcategory string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "armada_executor_job_failure_category_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["failure_category"] == category && labels["failure_subcategory"] == subcategory {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// failureCounterFamilyTotal sums the executor job failure counter across all
// label pairs, so tests can assert that no increment happened at all.
func failureCounterFamilyTotal(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	total := float64(0)
	for _, family := range families {
		if family.GetName() != "armada_executor_job_failure_category_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
	}
	return total
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
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
			UID:               types.UID(commonutil.NewULID()),
		},
		Spec: v1.PodSpec{
			NodeName: "node1",
		},
		Status: status,
	}
	return pod
}

func makePendingPodChecker() podchecks.PodChecker {
	var cfg podchecksConfig.Checks
	cfg.Events = []podchecksConfig.EventCheck{
		{Regexp: "Image pull has failed", Type: "Warning", GracePeriod: time.Nanosecond, Action: podchecksConfig.ActionFail},
		{Regexp: "Some other message", Type: "Warning", GracePeriod: time.Nanosecond, Action: podchecksConfig.ActionRetry},
	}
	cfg.ContainerStatuses = []podchecksConfig.ContainerStatusCheck{
		{State: podchecksConfig.ContainerStateWaiting, ReasonRegexp: "ImagePullBackOff", GracePeriod: time.Nanosecond, Action: podchecksConfig.ActionFail},
		{State: podchecksConfig.ContainerStateWaiting, ReasonRegexp: "Some reason", GracePeriod: time.Nanosecond, Action: podchecksConfig.ActionRetry},
	}

	checker, err := podchecks.NewPodChecks(cfg, realclock.RealClock{})
	if err != nil {
		panic(fmt.Sprintf("Failed to make pod checker: %v", err))
	}

	return checker
}

func makeFailedPodChecker() failedpodchecks.RetryChecker {
	checker, err := failedpodchecks.NewPodRetryChecker(podchecksConfig.FailedChecks{
		PodStatuses: []podchecksConfig.PodStatusCheck{
			{
				Regexp: fmt.Sprintf("^%s$", retryableFailedPodStatusMessage),
			},
		},
	})
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

func TestInternalSubcategoryForPodIssueType(t *testing.T) {
	tests := map[podIssueType]string{
		StuckTerminating:         errormatch.SubcategoryStuckTerminating,
		ExternallyDeleted:        errormatch.SubcategoryExternallyDeleted,
		ErrorDuringIssueHandling: errormatch.SubcategoryIssueHandlerError,
		ActiveDeadlineExceeded:   errormatch.SubcategoryActiveDeadline,
		StuckStartingUp:          "",
		UnableToSchedule:         "",
		FailedStartingUp:         "",
	}
	for issueType, want := range tests {
		assert.Equal(t, want, internalSubcategoryForPodIssueType(issueType))
	}
}

func TestDetectAndRegisterDeleteActionIssue(t *testing.T) {
	tests := map[string]struct {
		deleteAction     bool
		phase            v1.PodPhase
		podEventsErr     bool
		expectRegistered bool
	}{
		"failed pod in a delete-action category":  {deleteAction: true, phase: v1.PodFailed, expectRegistered: true},
		"failed pod in a retain category":         {deleteAction: false, phase: v1.PodFailed, expectRegistered: false},
		"running pod in a delete-action category": {deleteAction: true, phase: v1.PodRunning, expectRegistered: false},
		"pod events unavailable still registers":  {deleteAction: true, phase: v1.PodFailed, podEventsErr: true, expectRegistered: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			classifier := classifierForExitCode(t, "pih-del-cat", "pih-del-sub", 42, tc.deleteAction)
			podIssueService, _, fakeClusterContext, _, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
			require.NoError(t, err)
			pod := makeFailedPodWithExitCode(t, 42)
			pod.Status.Phase = tc.phase
			addPod(t, fakeClusterContext, pod)
			if tc.podEventsErr {
				fakeClusterContext.GetPodEventsErr = fmt.Errorf("events unavailable")
			}

			registered, err := podIssueService.DetectAndRegisterDeleteActionIssue(pod)
			require.NoError(t, err)
			assert.Equal(t, tc.expectRegistered, registered)
			assert.Equal(t, tc.expectRegistered, podIssueService.HasIssue(util.ExtractJobRunId(pod)))
		})
	}
}

func TestPodIssueService_DeleteAction_FailedPodChecksKeepPrecedence(t *testing.T) {
	// The pod matches both systems: its status message matches the failed pod
	// checker and its Evicted condition matches a delete-action category.
	classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{
			{
				Name:   "pih-evicted",
				Action: categorizer.PodFailureActionDelete,
				Rules:  []categorizer.CategoryRule{{OnConditions: []string{errormatch.ConditionEvicted}}},
			},
		},
	})
	require.NoError(t, err)
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
	require.NoError(t, err)
	pod := makeTestPod(v1.PodStatus{
		Phase:   v1.PodFailed,
		Reason:  errormatch.ConditionEvicted,
		Message: retryableFailedPodStatusMessage,
	})
	addPod(t, fakeClusterContext, pod)

	registered, err := podIssueService.DetectAndRegisterIssuesForFailedPod(pod)
	require.NoError(t, err)
	require.True(t, registered)

	podIssueService.HandlePodIssues()
	podIssueService.HandlePodIssues()
	events := eventsReporter.GetReceivedEvents()
	require.Len(t, events, 1)
	require.Len(t, events[0].Event.Events, 1)
	returnedEvent, ok := events[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	require.True(t, ok)
	require.Len(t, returnedEvent.JobRunErrors.Errors, 1)
	assert.NotNil(t, returnedEvent.JobRunErrors.Errors[0].GetPodLeaseReturned(),
		"the run must take the lease-return path, not the categorized terminal path")
}

type failingDeleteClusterContext struct {
	*fakecontext.SyncFakeClusterContext
	allowDeletes bool
}

func (c *failingDeleteClusterContext) DeletePodWithCondition(pod *v1.Pod, condition func(pod *v1.Pod) bool, pessimistic bool) error {
	if c.allowDeletes {
		return c.SyncFakeClusterContext.DeletePodWithCondition(pod, condition, pessimistic)
	}
	return fmt.Errorf("simulated delete failure")
}

func TestPodIssueService_DeleteAction_PreservesFailureCause(t *testing.T) {
	classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{
			{
				Name:   "pih-evicted",
				Action: categorizer.PodFailureActionDelete,
				Rules:  []categorizer.CategoryRule{{OnConditions: []string{errormatch.ConditionEvicted}}},
			},
		},
	})
	require.NoError(t, err)
	podIssueService, _, fakeClusterContext, eventsReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
	require.NoError(t, err)
	pod := makeTestPod(v1.PodStatus{Phase: v1.PodFailed, Reason: errormatch.ConditionEvicted})
	addPod(t, fakeClusterContext, pod)
	registered, err := podIssueService.DetectAndRegisterDeleteActionIssue(pod)
	require.NoError(t, err)
	require.True(t, registered)

	podIssueService.HandlePodIssues()
	podIssueService.HandlePodIssues()

	events := eventsReporter.GetReceivedEvents()
	require.Len(t, events, 1)
	failedEvent, ok := events[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	require.True(t, ok)
	require.Len(t, failedEvent.JobRunErrors.Errors, 1)
	podError := failedEvent.JobRunErrors.Errors[0].GetPodError()
	require.NotNil(t, podError)
	assert.Equal(t, armadaevents.KubernetesReason_Evicted, podError.KubernetesReason,
		"the reported cause must be the pod's real failure reason, not a generic app error")
}

// Each case drives HandlePodIssues through a sequence of cycles for a failed
// pod in a delete-action category and asserts the observable state after each.
func TestPodIssueService_DeleteActionLifecycle(t *testing.T) {
	type cycle struct {
		advanceTo      time.Duration // clock offset from the start, applied before the cycle
		allowDeletes   bool
		reporterErr    bool
		expectPodGone  bool
		expectEvents   int
		expectResolved bool
	}
	tests := map[string]struct {
		deleteFails       bool
		cycles            []cycle
		expectCategory    string
		expectSubcategory string
		expectMessage     string
	}{
		"deletes the pod first, reports the categorized failure once it is gone": {
			cycles: []cycle{
				{expectPodGone: true, expectEvents: 0},
				{expectPodGone: true, expectEvents: 1, expectResolved: true},
			},
			expectCategory:    "pih-del-cat",
			expectSubcategory: "pih-del-sub",
		},
		"a failed report is retried until delivered": {
			cycles: []cycle{
				{reporterErr: true, expectPodGone: true, expectEvents: 0},
				{reporterErr: true, expectPodGone: true, expectEvents: 0},
				{expectPodGone: true, expectEvents: 1, expectResolved: true},
			},
			expectCategory:    "pih-del-cat",
			expectSubcategory: "pih-del-sub",
		},
		// A pod that cannot be deleted within stuckTerminatingPodExpiry is
		// failed terminally as an internal stuck-terminating error instead of
		// staying invisible forever. The issue must stay registered while the
		// pod exists (resolving would let the reporter re-detect the pod and
		// report it again), and once the delete finally succeeds it resolves
		// without a second event.
		"an undeletable pod is failed terminally once, then resolves when the delete succeeds": {
			deleteFails: true,
			cycles: []cycle{
				{expectEvents: 0},
				{advanceTo: 4 * time.Minute, expectEvents: 1},
				{advanceTo: 8 * time.Minute, expectEvents: 1},
				{advanceTo: 8 * time.Minute, expectEvents: 1},
				{allowDeletes: true, expectPodGone: true, expectEvents: 1},
				{allowDeletes: true, expectPodGone: true, expectEvents: 1, expectResolved: true},
			},
			expectCategory:    errormatch.CategoryInternal,
			expectSubcategory: errormatch.SubcategoryStuckTerminating,
			expectMessage:     "could not be deleted",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			classifier := classifierForExitCode(t, "pih-del-cat", "pih-del-sub", 42, true)
			var clusterContext context.ClusterContext = fakecontext.NewSyncFakeClusterContext()
			var failingContext *failingDeleteClusterContext
			if tc.deleteFails {
				failingContext = &failingDeleteClusterContext{SyncFakeClusterContext: fakecontext.NewSyncFakeClusterContext()}
				clusterContext = failingContext
			}
			eventsReporter := mocks.NewFakeEventReporter()
			podIssueService, err := NewPodIssuerHandler(
				job.NewJobRunStateStoreWithInitialState([]*job.RunState{}),
				clusterContext,
				eventsReporter,
				configuration.StateChecksConfiguration{
					DeadlineForSubmittedPodConsideredMissing: time.Minute * 15,
					DeadlineForActivePodConsideredMissing:    time.Minute * 5,
				},
				makePendingPodChecker(),
				makeFailedPodChecker(),
				time.Minute*3,
				classifier,
			)
			require.NoError(t, err)
			baseTime := time.Now()
			fakeClock := clock.NewFakeClock(baseTime)
			podIssueService.clock = fakeClock
			pod := makeFailedPodWithExitCode(t, 42)
			addPod(t, clusterContext, pod)
			runId := util.ExtractJobRunId(pod)
			registered, err := podIssueService.DetectAndRegisterDeleteActionIssue(pod)
			require.NoError(t, err)
			require.True(t, registered)

			for i, c := range tc.cycles {
				fakeClock.SetTime(baseTime.Add(c.advanceTo))
				if failingContext != nil {
					failingContext.allowDeletes = c.allowDeletes
				}
				eventsReporter.ErrorOnReport = c.reporterErr
				podIssueService.HandlePodIssues()

				assert.Equal(t, !c.expectPodGone, len(getActivePods(t, clusterContext)) == 1, "cycle %d: pod presence", i)
				assert.Len(t, eventsReporter.GetReceivedEvents(), c.expectEvents, "cycle %d: events", i)
				assert.Equal(t, !c.expectResolved, podIssueService.HasIssue(runId), "cycle %d: issue state", i)
			}

			events := eventsReporter.GetReceivedEvents()
			require.NotEmpty(t, events)
			failedEvent, ok := events[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
			require.True(t, ok)
			jobError := failedEvent.JobRunErrors.Errors[0]
			assert.True(t, jobError.Terminal)
			assert.Equal(t, tc.expectCategory, jobError.FailureCategory)
			assert.Equal(t, tc.expectSubcategory, jobError.FailureSubcategory)
			if tc.expectMessage != "" {
				assert.Contains(t, jobError.GetPodError().GetMessage(), tc.expectMessage)
			}
		})
	}
}
