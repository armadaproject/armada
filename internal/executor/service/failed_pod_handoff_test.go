package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/internal/executor/categorizer"
	"github.com/armadaproject/armada/internal/executor/configuration"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestJobStateReporter_FailedPodDeleteActionHandoff(t *testing.T) {
	const hint = "The workload can be retried on another node."
	classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
		Categories: []categorizer.CategoryConfig{{
			Name:   "infrastructure",
			Action: categorizer.PodFailureActionDelete,
			Rules: []categorizer.CategoryRule{{
				OnPodEvents: &errormatch.PodEventMatcher{Type: "Warning", Regexp: "node lost"},
				Subcategory: "node-lost",
				Hint:        hint,
			}},
		}},
	})
	require.NoError(t, err)
	cluster := &deferredDeleteClusterContext{SyncFakeClusterContext: fakecontext.NewSyncFakeClusterContext()}
	events := mocks.NewFakeEventReporter()
	renderer := reporter.NewDebugMessageRenderer(cluster, testDebugConfig())
	handler, err := NewPodIssuerHandler(job.NewJobRunStateStoreWithInitialState(nil), cluster, events,
		configuration.StateChecksConfiguration{}, makePendingPodChecker(), makeFailedPodChecker(),
		3*time.Minute, 30*time.Second, classifier, renderer)
	require.NoError(t, err)
	stateReporter, err := NewJobStateReporter(cluster, events, handler, classifier, renderer, nil, nil, time.Minute)
	require.NoError(t, err)

	pod := makeTestPod(v1.PodStatus{Phase: v1.PodFailed, Reason: "Evicted", Message: "Node was lost"})
	addPod(t, cluster, pod)
	addPodEvents(cluster.SyncFakeClusterContext, pod, []*v1.Event{{Type: "Warning", Message: "node lost"}})
	runId := util.ExtractJobRunId(pod)
	// Call synchronously: this test covers serial reconciliation, not informer goroutine races.
	stateReporter.reportCurrentStatus(pod)
	require.True(t, handler.HasIssue(runId))
	require.Empty(t, events.GetReceivedEvents(), "registration must suppress the ordinary failure report")
	require.Empty(t, cluster.deleteRequests)

	// makeTestPod sets creation ten minutes ago. With no later status changes and no
	// pending events, this pod is eligible for every missing-event reconciliation.
	require.True(t, util.HasPodBeenInStateForLongerThanGivenDuration(pod, 30*time.Second))
	require.False(t, events.HasPendingEvents(pod))
	for i := 0; i < 2; i++ {
		stateReporter.ReportMissingJobEvents()
		require.True(t, handler.HasIssue(runId))
		require.Empty(t, events.GetReceivedEvents())
	}

	handler.HandlePodIssues()
	require.Len(t, cluster.deleteRequests, 1)
	assert.Equal(t, runId, util.ExtractJobRunId(cluster.deleteRequests[0]))
	require.Len(t, getActivePods(t, cluster), 1, "delete acceptance does not imply observed absence")
	for i := 0; i < 2; i++ {
		stateReporter.ReportMissingJobEvents()
		handler.HandlePodIssues()
		require.True(t, handler.HasIssue(runId))
		require.Empty(t, events.GetReceivedEvents(), "an owned, still visible pod must not report failure")
	}

	// Later cluster observation sees the pod gone. Its events are no longer available;
	// the terminal report must retain the classification captured at registration.
	cluster.DeletePods([]*v1.Pod{pod})
	delete(cluster.Events, util.ExtractJobId(pod))
	for i := 0; i < 2; i++ {
		stateReporter.ReportMissingJobEvents()
		handler.HandlePodIssues()
	}
	require.False(t, handler.HasIssue(runId))
	received := events.GetReceivedEvents()
	require.Len(t, received, 1)
	assert.Equal(t, runId, received[0].JobRunId)
	require.Len(t, received[0].Event.Events, 1)
	runErrors := received[0].Event.Events[0].GetJobRunErrors()
	require.NotNil(t, runErrors)
	require.Len(t, runErrors.Errors, 1)
	runError := runErrors.Errors[0]
	assert.True(t, runError.Terminal)
	assert.Equal(t, "infrastructure", runError.FailureCategory)
	assert.Equal(t, "node-lost", runError.FailureSubcategory)
	podError := runError.GetPodError()
	require.NotNil(t, podError)
	assert.Equal(t, armadaevents.KubernetesReason_Evicted, podError.KubernetesReason)
	assert.Equal(t, "Node was lost\n\n"+hint, podError.Message)
}

// Accept deletion while retaining the pod in the observed cluster state until the
// test removes it explicitly. This models the boundary, not Kubernetes watch delivery.
type deferredDeleteClusterContext struct {
	*fakecontext.SyncFakeClusterContext
	deleteRequests []*v1.Pod
}

func (c *deferredDeleteClusterContext) DeletePodWithCondition(pod *v1.Pod, condition func(*v1.Pod) bool, pessimistic bool) error {
	if !condition(pod) {
		return fmt.Errorf("pod does not match deletion condition")
	}
	c.deleteRequests = append(c.deleteRequests, pod.DeepCopy())
	return nil
}
