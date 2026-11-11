package service

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/internal/executor/categorizer"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestPodIssueService_NonRetryableReportRecovery(t *testing.T) {
	for _, category := range []string{"", "recovery-default"} {
		for _, buildFailure := range []bool{true, false} {
			t.Run(fmt.Sprintf("category=%q/buildFailure=%t", category, buildFailure), func(t *testing.T) {
				classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{DefaultCategory: category})
				require.NoError(t, err)
				handler, _, fakeCluster, events, err := setupTestComponentsWithClassifier(nil, classifier)
				require.NoError(t, err)
				cluster := &reportRecoveryCluster{SyncFakeClusterContext: fakeCluster}
				handler.clusterContext = cluster
				reports := &reportAttempts{FakeEventReporter: events}
				handler.eventReporter = reports
				pod := makeUnretryableStuckPod()
				addPod(t, cluster, pod)
				// Use real pending-pod detection, capturing its category before reporting.
				handler.detectPodIssues([]*v1.Pod{pod})
				runId := util.ExtractJobRunId(pod)
				registered := handler.knownPodIssues[runId]
				require.NotNil(t, registered)
				require.False(t, registered.PodIssue.Retryable)
				require.Equal(t, category, registered.PodIssue.Classification.Category)
				before := failureCounterValue(t, category, "")
				if buildFailure {
					// Missing labels exercise the event builder's defensive failure path;
					// this is not a claim that managed production pods normally lack job ids.
					delete(registered.PodIssue.OriginalPodState.Labels, domain.JobId)
				} else {
					events.ErrorOnReport = true
				}

				handler.HandlePodIssues()
				require.True(t, handler.HasIssue(runId))
				assert.False(t, registered.Reported)
				assert.False(t, registered.PodIssue.DeletionRequested)
				assert.Zero(t, cluster.deletes)
				require.Len(t, getActivePods(t, cluster), 1)
				assert.Empty(t, events.GetReceivedEvents())
				if buildFailure {
					assert.Empty(t, reports.attempts)
				} else {
					assert.Len(t, reports.attempts, 1)
				}
				assert.Equal(t, before, failureCounterValue(t, category, ""))

				registered.PodIssue.OriginalPodState.Labels[domain.JobId] = util.ExtractJobId(pod)
				events.ErrorOnReport = false
				handler.HandlePodIssues()
				require.True(t, handler.HasIssue(runId), "accepted deletion is not observed absence")
				assert.True(t, registered.Reported)
				assert.True(t, registered.PodIssue.DeletionRequested)
				assert.Equal(t, 1, cluster.deletes)
				require.Len(t, events.GetReceivedEvents(), 1)
				assert.Equal(t, category, receivedFailure(t, events.GetReceivedEvents()[0]).FailureCategory)
				delta := float64(1)
				if category == "" {
					delta = 0
				}
				assert.Equal(t, before+delta, failureCounterValue(t, category, ""))

				handler.HandlePodIssues()
				cluster.SyncFakeClusterContext.DeletePods([]*v1.Pod{pod})
				handler.HandlePodIssues()
				handler.HandlePodIssues()
				assert.False(t, handler.HasIssue(runId))
				assert.Len(t, events.GetReceivedEvents(), 1)
				attempts := 1
				if !buildFailure {
					attempts++
				}
				assert.Len(t, reports.attempts, attempts, "success prevents another report")
				assert.Equal(t, before+delta, failureCounterValue(t, category, ""))
			})
		}
	}
}

func TestPodIssueService_DeleteActionFailedEscalationRecovery(t *testing.T) {
	for _, buildFailure := range []bool{true, false} {
		t.Run(fmt.Sprintf("buildFailure=%t", buildFailure), func(t *testing.T) {
			const category, subcategory, hint = "recovery-delete", "node-lost", "Retry on another node."
			classifier, err := categorizer.NewClassifier(categorizer.ErrorCategoriesConfig{
				Categories: []categorizer.CategoryConfig{{
					Name: category, Action: categorizer.PodFailureActionDelete,
					Rules: []categorizer.CategoryRule{{OnPodEvents: &errormatch.PodEventMatcher{Type: "Warning", Regexp: "node lost"}, Subcategory: subcategory, Hint: hint}},
				}},
			})
			require.NoError(t, err)
			handler, _, fakeCluster, events, err := setupTestComponentsWithClassifier(nil, classifier)
			require.NoError(t, err)
			cluster := &reportRecoveryCluster{SyncFakeClusterContext: fakeCluster}
			handler.clusterContext = cluster
			reports := &reportAttempts{FakeEventReporter: events}
			handler.eventReporter = reports
			now := time.Now()
			fakeClock := clock.NewFakeClock(now)
			handler.clock = fakeClock
			pod := makeFailedPodWithExitCode(t, 42)
			pod.Name, pod.Namespace = "original-pod", "workloads"
			pod.Status.Reason, pod.Status.Message = "Evicted", "Node was lost"
			pod.Status.ContainerStatuses[0].Name = "worker"
			pod.Status.ContainerStatuses[0].State.Terminated.Message = "original container failure"
			addPod(t, cluster, pod)
			addPodEvents(fakeCluster, pod, []*v1.Event{{Type: "Warning", Message: "node lost"}})
			registered, err := handler.DetectAndRegisterIssuesForFailedPod(pod)
			require.NoError(t, err)
			require.True(t, registered)
			runId := util.ExtractJobRunId(pod)
			stored := handler.knownPodIssues[runId]
			require.Equal(t, category, stored.PodIssue.Classification.Category)
			initialDebug := stored.PodIssue.DebugMessage
			originalMetric := failureCounterValue(t, category, subcategory)
			escalationMetric := failureCounterValue(t, errormatch.CategoryInternal, errormatch.SubcategoryStuckTerminating)

			// Detection was recent, but the observed deletion timestamp is overdue.
			// The timestamp must take precedence over detection-time fallback.
			current := pod.DeepCopy()
			deletedAt := metav1.NewTime(now.Add(-4 * time.Minute))
			current.DeletionTimestamp = &deletedAt
			current.Finalizers = []string{"example.com/hold"}
			current.Status.Reason = "DifferentCurrentReason"
			current.Status.ContainerStatuses[0].State.Terminated.ExitCode = 99
			addPod(t, cluster, current)
			addPodEvents(fakeCluster, pod, []*v1.Event{{Type: "Warning", Message: "deletion blocked by finalizer"}})
			if buildFailure {
				delete(stored.PodIssue.OriginalPodState.Labels, domain.JobId)
			} else {
				events.ErrorOnReport = true
			}
			handler.HandlePodIssues()
			require.True(t, handler.HasIssue(runId))
			assert.False(t, stored.Reported)
			assert.True(t, stored.PodIssue.DeletionRequested, "escalation failure must still attempt deletion")
			assert.Equal(t, 1, cluster.deletes)
			require.Len(t, getActivePods(t, cluster), 1)
			assert.Empty(t, events.GetReceivedEvents())
			assert.Equal(t, originalMetric, failureCounterValue(t, category, subcategory))
			assert.Equal(t, escalationMetric, failureCounterValue(t, errormatch.CategoryInternal, errormatch.SubcategoryStuckTerminating))
			refreshedDebug := stored.PodIssue.DebugMessage
			assert.NotEqual(t, initialDebug, refreshedDebug)
			debug := &reporter.PodDebugInfo{}
			require.NoError(t, json.Unmarshal([]byte(refreshedDebug), debug))
			assert.Equal(t, reporter.TriggerUndeletable, debug.Trigger)
			assert.Equal(t, []string{"example.com/hold"}, debug.Pod.Finalizers)
			assert.NotEmpty(t, debug.Pod.DeletionTimestamp)
			require.Len(t, debug.Pod.Events, 1)
			assert.Equal(t, "deletion blocked by finalizer", debug.Pod.Events[0].Message)
			if buildFailure {
				assert.Empty(t, reports.attempts)
			} else {
				require.Len(t, reports.attempts, 1)
				attempted := receivedFailure(t, reports.attempts[0])
				assert.Equal(t, errormatch.CategoryInternal, attempted.FailureCategory)
				assert.Equal(t, errormatch.SubcategoryStuckTerminating, attempted.FailureSubcategory)
				assert.Contains(t, attempted.GetPodError().Message, "could not be deleted")
			}

			// Observe absence separately from accepted deletion. Events and current pod
			// data are unavailable now; the retry uses the stored original failure.
			cluster.SyncFakeClusterContext.DeletePods([]*v1.Pod{current})
			delete(fakeCluster.Events, util.ExtractJobId(pod))
			stored.PodIssue.OriginalPodState.Labels[domain.JobId] = util.ExtractJobId(pod)
			events.ErrorOnReport = false
			handler.HandlePodIssues()
			require.True(t, stored.Reported)
			require.False(t, handler.HasIssue(runId))
			require.Len(t, events.GetReceivedEvents(), 1)
			message := events.GetReceivedEvents()[0]
			assert.Equal(t, runId, message.JobRunId)
			runErrors := message.Event.Events[0].GetJobRunErrors()
			assert.Equal(t, util.ExtractJobId(pod), runErrors.JobId)
			assert.Equal(t, runId, runErrors.RunId)
			failure := receivedFailure(t, message)
			assert.True(t, failure.Terminal)
			assert.Equal(t, category, failure.FailureCategory)
			assert.Equal(t, subcategory, failure.FailureSubcategory)
			payload := failure.GetPodError()
			require.NotNil(t, payload)
			assert.Equal(t, "Node was lost\n\n"+hint, payload.Message)
			assert.Equal(t, armadaevents.KubernetesReason_Evicted, payload.KubernetesReason)
			assert.Equal(t, &armadaevents.ObjectMeta{KubernetesId: string(pod.UID), Namespace: pod.Namespace, Name: pod.Name, ExecutorId: cluster.GetClusterId()}, payload.ObjectMeta)
			assert.Equal(t, pod.Spec.NodeName, payload.NodeName)
			require.Len(t, payload.ContainerErrors, 1)
			assert.Equal(t, int32(42), payload.ContainerErrors[0].ExitCode)
			assert.Equal(t, "original container failure", payload.ContainerErrors[0].Message)
			assert.Equal(t, "Error", payload.ContainerErrors[0].Reason)
			assert.Equal(t, armadaevents.KubernetesReason_AppError, payload.ContainerErrors[0].KubernetesReason)
			assert.Equal(t, &armadaevents.ObjectMeta{Name: "worker", Namespace: pod.Namespace, ExecutorId: cluster.GetClusterId()}, payload.ContainerErrors[0].ObjectMeta)
			assert.Equal(t, refreshedDebug, payload.DebugMessage)
			assert.Equal(t, originalMetric+1, failureCounterValue(t, category, subcategory))
			assert.Equal(t, escalationMetric, failureCounterValue(t, errormatch.CategoryInternal, errormatch.SubcategoryStuckTerminating))
			handler.HandlePodIssues()
			assert.Len(t, events.GetReceivedEvents(), 1)
			attempts := 1
			if !buildFailure {
				attempts++
			}
			assert.Len(t, reports.attempts, attempts)
			assert.Equal(t, 1, cluster.deletes)
			assert.Equal(t, originalMetric+1, failureCounterValue(t, category, subcategory))
		})
	}
}

// Deletion is accepted but remains visible until the test explicitly removes it.
// Calls are synchronous at these seams; no Kubernetes delivery guarantee is tested.
type reportRecoveryCluster struct {
	*fakecontext.SyncFakeClusterContext
	deletes int
}

func (c *reportRecoveryCluster) DeletePods(pods []*v1.Pod) { c.deletes += len(pods) }
func (c *reportRecoveryCluster) DeletePodWithCondition(pod *v1.Pod, condition func(*v1.Pod) bool, pessimistic bool) error {
	if !condition(pod) {
		return fmt.Errorf("pod does not match deletion condition")
	}
	c.deletes++
	return nil
}

type reportAttempts struct {
	*mocks.FakeEventReporter
	attempts []reporter.EventMessage
}

func (r *reportAttempts) Report(events []reporter.EventMessage) error {
	r.attempts = append(r.attempts, events...)
	return r.FakeEventReporter.Report(events)
}

func receivedFailure(t *testing.T, message reporter.EventMessage) *armadaevents.Error {
	t.Helper()
	require.Len(t, message.Event.Events, 1)
	errors := message.Event.Events[0].GetJobRunErrors()
	require.NotNil(t, errors)
	require.Len(t, errors.Errors, 1)
	return errors.Errors[0]
}
