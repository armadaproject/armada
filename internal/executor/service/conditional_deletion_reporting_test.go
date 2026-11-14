package service

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"

	"github.com/armadaproject/armada/internal/executor/configuration"
	clustercontext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type deletionReportingClient struct{ client *fake.Clientset }

func (p deletionReportingClient) Client() kubernetes.Interface { return p.client }
func (p deletionReportingClient) ClientForUser(string, []string) (kubernetes.Interface, error) {
	return p.client, nil
}
func (p deletionReportingClient) ClientConfig() *rest.Config { return nil }

// Freeze the informer at the decision snapshot. Only the reconciler's listing is
// supplied directly: both deletion and reporting use the real implementations.
type deletionReportingContext struct {
	clustercontext.ClusterContext
	pods []*v1.Pod
}

func (c *deletionReportingContext) GetActiveBatchPods() ([]*v1.Pod, error) { return c.pods, nil }

func useDeletionReportingRegistry(t *testing.T) {
	t.Helper()
	previous := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = previous })
}

func TestConditionalDeletionLifecycleReporting(t *testing.T) {
	denied := apierrors.NewForbidden(v1.Resource("pods"), "conditional-delete", errors.New("delete denied"))
	conflict := apierrors.NewConflict(v1.Resource("pods"), "conditional-delete", errors.New("pod updated"))
	for _, tc := range []struct {
		name                   string
		deleteError            error
		accepted               bool
		suppressedBeforeReject bool
	}{
		{name: "rejected DELETE restores reporting", deleteError: denied},
		{name: "DELETE conflict restores reporting", deleteError: conflict},
		{name: "accepted DELETE retains suppression", accepted: true},
		{name: "lost DELETE response retains suppression", accepted: true, deleteError: apierrors.NewTimeoutError("response lost", 0)},
		{name: "missed phase needs reconciliation", deleteError: conflict, suppressedBeforeReject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stateReporter, _, events, _ := setUpJobStateReporterTest(t)
			stateReporter.startUpTimings = nil
			useDeletionReportingRegistry(t)
			pod := makeTestPod(v1.PodStatus{Phase: v1.PodPending})
			pod.Name, pod.Namespace, pod.ResourceVersion = "conditional-delete", "default", "1"
			client := fake.NewSimpleClientset(pod.DeepCopy())
			realContext := clustercontext.NewClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", DeleteConcurrencyLimit: 1}, time.Minute, deletionReportingClient{client}, time.Minute)
			realContext.Stop()
			ctx := &deletionReportingContext{ClusterContext: realContext}
			stateReporter.clusterContext = ctx
			gvr := v1.SchemeGroupVersion.WithResource("pods")
			getLive := func() *v1.Pod {
				obj, err := client.Tracker().Get(gvr, pod.Namespace, pod.Name)
				require.NoError(t, err)
				return obj.(*v1.Pod)
			}
			var suppressed *v1.Pod
			client.PrependReactor("delete", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
				live := getLive()
				if tc.accepted {
					now := metav1.Now()
					live.DeletionTimestamp = &now
				}
				if tc.suppressedBeforeReject {
					live.Status.Phase = v1.PodRunning
					live.ResourceVersion = "2"
					suppressed = live.DeepCopy()
					stateReporter.reportStatusUpdate(pod, suppressed)
					require.Empty(t, events.GetReceivedEvents())
				}
				require.NoError(t, client.Tracker().Update(gvr, live, pod.Namespace))
				return true, nil, tc.deleteError
			})
			err := realContext.DeletePodWithCondition(pod, func(p *v1.Pod) bool { return p.Status.Phase == v1.PodPending }, true)
			require.ErrorIs(t, err, tc.deleteError)
			running := getLive().DeepCopy()
			running.Status.Phase = v1.PodRunning
			if tc.suppressedBeforeReject {
				stateReporter.reportStatusUpdate(suppressed, running)
				require.Empty(t, events.GetReceivedEvents(), "marker removal does not replay the suppressed phase change")
				ctx.pods = []*v1.Pod{running}
				stateReporter.ReportMissingJobEvents()
			} else {
				stateReporter.reportStatusUpdate(pod, running)
			}
			if tc.accepted {
				require.Empty(t, events.GetReceivedEvents(), "intentional deletion still suppresses phase updates")
			} else {
				require.Len(t, events.GetReceivedEvents(), 1)
				require.IsType(t, &armadaevents.EventSequence_Event_JobRunRunning{}, events.GetReceivedEvents()[0].Event.Events[0].Event)
			}
			assert.Equal(t, tc.accepted, util.IsMarkedForDeletion(running))
		})
	}
}

func TestConditionalDeletion_UnmarkedTerminatingPodKeepsIntentionalReporting(t *testing.T) {
	stateReporter, _, events, _ := setUpJobStateReporterTest(t)
	useDeletionReportingRegistry(t)
	pod := makeTestPod(v1.PodStatus{Phase: v1.PodPending})
	pod.Name, pod.Namespace, pod.ResourceVersion = "already-terminating", "default", "1"
	pod.DeletionTimestamp = &metav1.Time{Time: time.Now().Add(-time.Minute)}
	client := fake.NewSimpleClientset(pod.DeepCopy())
	ctx := clustercontext.NewClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", DeleteConcurrencyLimit: 1}, time.Minute, deletionReportingClient{client}, 5*time.Minute)
	t.Cleanup(ctx.Stop)
	stateReporter.clusterContext = ctx
	require.NoError(t, ctx.DeletePodWithCondition(pod, func(p *v1.Pod) bool { return p.Status.Phase == v1.PodPending }, true))
	obj, err := client.Tracker().Get(v1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name)
	require.NoError(t, err)
	terminating := obj.(*v1.Pod)
	failed := terminating.DeepCopy()
	failed.Status.Phase = v1.PodFailed
	stateReporter.reportStatusUpdate(terminating, failed)
	assert.Empty(t, events.GetReceivedEvents(), "shutdown after intentional handling must remain suppressed")
	handler, _, _, _, err := setupTestComponents(nil)
	require.NoError(t, err)
	handler.handleDeletedPod(failed)
	assert.False(t, handler.HasIssue(util.ExtractJobRunId(pod)), "marked deletion must not become an unexpected-deletion issue")
	// Confirm the callback check is meaningful independently of the phase.
	unmarked := failed.DeepCopy()
	delete(unmarked.Annotations, domain.MarkedForDeletion)
	handler.handleDeletedPod(unmarked)
	require.True(t, handler.HasIssue(util.ExtractJobRunId(pod)))
}
