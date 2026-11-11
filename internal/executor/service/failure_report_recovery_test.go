package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/internal/executor/categorizer"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// A failed report must leave the issue registered, the pod in place and the
// counter untouched, so a later pass reports it exactly once.
func TestPodIssueService_NonRetryableIssue_ReportFailureDefersDeletion(t *testing.T) {
	category := "pih-nonretry-report-fail-cat"
	classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{DefaultCategory: category})
	require.NoError(t, err)
	podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
	require.NoError(t, err)
	counterBefore := failureCounterValue(t, category, "")

	pod := makeUnretryableStuckPod()
	addPod(t, fakeClusterContext, pod)

	eventReporter.ErrorOnReport = true
	podIssueService.HandlePodIssues()

	assert.True(t, podIssueService.HasIssue(util.ExtractJobRunId(pod)))
	assert.Len(t, getActivePods(t, fakeClusterContext), 1, "the pod must not be deleted before its failure is reported")
	assert.Empty(t, eventReporter.GetReceivedEvents())
	assert.Equal(t, counterBefore, failureCounterValue(t, category, ""), "failed sends must not increment the counter")

	eventReporter.ErrorOnReport = false
	podIssueService.HandlePodIssues()
	podIssueService.HandlePodIssues()

	assert.Empty(t, getActivePods(t, fakeClusterContext))
	events := eventReporter.GetReceivedEvents()
	require.Len(t, events, 1)
	failedEvent, ok := events[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	require.True(t, ok)
	assert.Equal(t, category, failedEvent.JobRunErrors.Errors[0].GetFailureCategory())
	assert.Equal(t, counterBefore+1, failureCounterValue(t, category, ""))
}

// If the stuck-terminating escalation cannot be reported, deletion must still
// go ahead. Once the pod is gone the original categorized failure is reported,
// not the escalation, because a scheduler retry can no longer collide with it.
func TestPodIssueService_DeleteAction_FailedEscalationReportsOriginalFailureOncePodIsGone(t *testing.T) {
	category, subcategory := "pih-del-escalation-cat", "pih-del-escalation-sub"
	classifier := classifierForExitCode(t, category, subcategory, 42, true)
	podIssueService, _, fakeClusterContext, eventReporter, err := setupTestComponentsWithClassifier([]*job.RunState{}, classifier)
	require.NoError(t, err)
	originalBefore := failureCounterValue(t, category, subcategory)
	escalationBefore := failureCounterValue(t, errormatch.CategoryInternal, errormatch.SubcategoryStuckTerminating)

	pod := makeFailedPodWithExitCode(t, 42)
	addPod(t, fakeClusterContext, pod)
	registered, err := podIssueService.DetectAndRegisterDeleteActionIssue(pod)
	require.NoError(t, err)
	require.True(t, registered)
	runId := util.ExtractJobRunId(pod)

	// The pod has been terminating for longer than stuckTerminatingPodExpiry, so this pass escalates.
	deletedAt := metav1.NewTime(time.Now().Add(-4 * time.Minute))
	pod.DeletionTimestamp = &deletedAt

	eventReporter.ErrorOnReport = true
	podIssueService.HandlePodIssues()

	assert.Empty(t, getActivePods(t, fakeClusterContext), "a failed escalation report must not block deletion")
	assert.Empty(t, eventReporter.GetReceivedEvents())
	assert.True(t, podIssueService.HasIssue(runId))

	eventReporter.ErrorOnReport = false
	podIssueService.HandlePodIssues()
	podIssueService.HandlePodIssues()

	assert.False(t, podIssueService.HasIssue(runId))
	events := eventReporter.GetReceivedEvents()
	require.Len(t, events, 1)
	failedEvent, ok := events[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	require.True(t, ok)
	jobError := failedEvent.JobRunErrors.Errors[0]
	assert.True(t, jobError.Terminal)
	assert.Equal(t, category, jobError.FailureCategory)
	assert.Equal(t, subcategory, jobError.FailureSubcategory)
	assert.Equal(t, originalBefore+1, failureCounterValue(t, category, subcategory))
	assert.Equal(t, escalationBefore, failureCounterValue(t, errormatch.CategoryInternal, errormatch.SubcategoryStuckTerminating))
}
