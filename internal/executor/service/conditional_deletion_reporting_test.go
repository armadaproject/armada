package service

import (
	"errors"
	"fmt"
	"reflect"
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

func TestConditionalDeletionLifecycleReporting(t *testing.T) {
	for _, scenario := range []string{
		"live recovery", "GET failure", "PATCH failure", "PATCH conflict",
		"DELETE conflict", "DELETE failure", "accepted deletion", "concurrent cancellation",
		"cancellation after PATCH", "PATCH committed response lost", "DELETE committed response lost",
		"DELETE conflict after suppressed update",
	} {
		t.Run(scenario, func(t *testing.T) {
			stateReporter, _, events, _ := setUpJobStateReporterTest(t)
			stateReporter.startUpTimings = nil
			previousRegisterer := prometheus.DefaultRegisterer
			prometheus.DefaultRegisterer = prometheus.NewRegistry()
			t.Cleanup(func() { prometheus.DefaultRegisterer = previousRegisterer })
			pod := makeTestPod(v1.PodStatus{Phase: v1.PodPending})
			pod.Name, pod.Namespace, pod.ResourceVersion = "conditional-delete", "default", "1"
			client := fake.NewSimpleClientset(pod.DeepCopy())
			realContext := clustercontext.NewClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", DeleteConcurrencyLimit: 1, EnableConditionalDeletionMarkerCleanup: true}, time.Minute, deletionReportingClient{client}, time.Minute)
			realContext.Stop()
			ctx := &deletionReportingContext{ClusterContext: realContext}
			stateReporter.clusterContext = ctx
			gvr := v1.SchemeGroupVersion.WithResource("pods")
			getLive := func() *v1.Pod {
				obj, err := client.Tracker().Get(gvr, pod.Namespace, pod.Name)
				require.NoError(t, err)
				return obj.(*v1.Pod)
			}
			updateLive := func(p *v1.Pod) { require.NoError(t, client.Tracker().Update(gvr, p, p.Namespace)) }
			injected := errors.New("injected API failure")
			deletionAttempt := true
			var suppressed *v1.Pod
			client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
				if scenario == "GET failure" {
					return true, nil, injected
				}
				if scenario == "live recovery" || scenario == "concurrent cancellation" {
					live := getLive()
					live.Status.Phase = v1.PodRunning
					live.ResourceVersion = "2"
					if scenario == "concurrent cancellation" {
						live.Annotations[domain.MarkedForDeletion] = "cancellation-owner"
					}
					updateLive(live)
				}
				return false, nil, nil
			})
			client.PrependReactor("patch", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
				if deletionAttempt && scenario == "PATCH committed response lost" {
					live := getLive()
					live.Annotations[domain.MarkedForDeletion] = "committed-patch"
					updateLive(live)
					return true, nil, injected
				}
				if deletionAttempt && scenario == "PATCH failure" {
					return true, nil, injected
				}
				if deletionAttempt && scenario == "PATCH conflict" {
					return true, nil, apierrors.NewConflict(v1.Resource("pods"), pod.Name, injected)
				}
				return false, nil, nil
			})
			client.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if scenario == "cancellation after PATCH" {
					live := getLive()
					live.Annotations[domain.MarkedForDeletion] = "cancellation-owner"
					now := metav1.Now()
					live.DeletionTimestamp = &now
					updateLive(live)
					return true, nil, apierrors.NewConflict(v1.Resource("pods"), pod.Name, injected)
				}
				if scenario == "DELETE conflict" || scenario == "DELETE conflict after suppressed update" {
					options := action.(clienttesting.DeleteAction).GetDeleteOptions()
					require.NotNil(t, options.Preconditions.ResourceVersion)
					live := getLive()
					live.Status.Phase = v1.PodRunning
					live.ResourceVersion = "3"
					updateLive(live)
					require.NotEqual(t, live.ResourceVersion, *options.Preconditions.ResourceVersion)
					if scenario == "DELETE conflict after suppressed update" {
						suppressed = live.DeepCopy()
						stateReporter.reportStatusUpdate(pod, suppressed)
						require.Empty(t, events.GetReceivedEvents())
					}
					return true, nil, apierrors.NewConflict(v1.Resource("pods"), pod.Name, injected)
				}
				if scenario == "DELETE failure" {
					return true, nil, apierrors.NewForbidden(v1.Resource("pods"), pod.Name, injected)
				}
				// Model an accepted graceful deletion while the object remains visible.
				live := getLive()
				now := metav1.Now()
				live.DeletionTimestamp = &now
				updateLive(live)
				if scenario == "DELETE committed response lost" {
					return true, nil, injected
				}
				return true, nil, nil
			})
			err := realContext.DeletePodWithCondition(pod, func(p *v1.Pod) bool { return p.Status.Phase == v1.PodPending }, true)
			if scenario == "accepted deletion" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			deletionAttempt = false
			live := getLive()
			intentional := scenario == "accepted deletion" || scenario == "concurrent cancellation" || scenario == "cancellation after PATCH" || scenario == "PATCH committed response lost" || scenario == "DELETE committed response lost"
			if scenario == "concurrent cancellation" || scenario == "cancellation after PATCH" {
				require.Equal(t, "cancellation-owner", live.Annotations[domain.MarkedForDeletion])
			}
			// Lost-response cases characterize retained suppression under uncertainty;
			// they do not assert that the abandoned PATCH is correct or recovered.
			// Assert observable reporting before checking annotation implementation details.
			running := live.DeepCopy()
			running.Status.Phase = v1.PodRunning
			updateLive(running)
			if suppressed != nil {
				stateReporter.reportStatusUpdate(suppressed, running)
				require.Empty(t, events.GetReceivedEvents(), "removing a marker does not replay the suppressed phase change")
				ctx.pods = []*v1.Pod{running}
				stateReporter.ReportMissingJobEvents()
			} else {
				stateReporter.reportStatusUpdate(pod, running)
			}
			normalEvents := 1
			if intentional {
				normalEvents = 0
			}
			assert.Len(t, events.GetReceivedEvents(), normalEvents, "ordinary Running phase reporting eligibility")
			if len(events.GetReceivedEvents()) == 1 {
				assertExpectedEvents(t, pod, events.GetReceivedEvents(), reflect.TypeOf(&armadaevents.EventSequence_Event_JobRunRunning{}))
			}
			assert.Equal(t, intentional, util.IsMarkedForDeletion(live))
			// Reconciliation bypasses the marker. If Running was emitted, mimic its
			// successful annotation; otherwise retain it as missing and reconcile it.
			if len(events.GetReceivedEvents()) == 1 {
				running.Annotations[string(v1.PodRunning)] = "reported"
			}
			ctx.pods = []*v1.Pod{running}
			stateReporter.ReportMissingJobEvents()
			require.Len(t, events.GetReceivedEvents(), 1, "a retained available pod can have its current phase reconciled")
			succeeded := running.DeepCopy()
			succeeded.Status.Phase = v1.PodSucceeded
			stateReporter.reportStatusUpdate(running, succeeded)
			want := 2
			if intentional {
				want = 1
			}
			assert.Len(t, events.GetReceivedEvents(), want, "ordinary terminal phase reporting eligibility")
			if len(events.GetReceivedEvents()) == 2 {
				require.IsType(t, &armadaevents.EventSequence_Event_JobRunSucceeded{}, events.GetReceivedEvents()[1].Event.Events[0].Event)
			}
			if len(events.GetReceivedEvents()) == 2 {
				succeeded.Annotations[string(v1.PodSucceeded)] = "reported"
			}
			ctx.pods = []*v1.Pod{succeeded}
			stateReporter.ReportMissingJobEvents()
			require.Len(t, events.GetReceivedEvents(), 2, "available terminal state remains reconcilable")
			require.IsType(t, &armadaevents.EventSequence_Event_JobRunSucceeded{}, events.GetReceivedEvents()[1].Event.Events[0].Event)
		})
	}
}

func TestConditionalDeletion_UnmarkedTerminatingPodKeepsIntentionalReporting(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanup=%t", enabled), func(t *testing.T) {
			stateReporter, _, events, _ := setUpJobStateReporterTest(t)
			previous := prometheus.DefaultRegisterer
			prometheus.DefaultRegisterer = prometheus.NewRegistry()
			t.Cleanup(func() { prometheus.DefaultRegisterer = previous })
			pod := makeTestPod(v1.PodStatus{Phase: v1.PodPending})
			pod.Name, pod.Namespace, pod.ResourceVersion = "already-terminating", "default", "1"
			pod.DeletionTimestamp = &metav1.Time{Time: time.Now().Add(-time.Minute)}
			client := fake.NewSimpleClientset(pod.DeepCopy())
			ctx := clustercontext.NewClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", DeleteConcurrencyLimit: 1, EnableConditionalDeletionMarkerCleanup: enabled}, time.Minute, deletionReportingClient{client}, 5*time.Minute)
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
		})
	}
}
