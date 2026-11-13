package context

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
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
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"

	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
)

// Block outside client-go Fake's reaction mutex so other contexts can genuinely
// issue API calls while this context has an outstanding DELETE.
type recoveryClient struct {
	kubernetes.Interface
	beforeDelete func(metav1.DeleteOptions) error
}

func (c recoveryClient) IsWatchListSemanticsUnSupported() bool { return true }
func (c recoveryClient) CoreV1() typedcore.CoreV1Interface {
	return recoveryCore{c.Interface.CoreV1(), c.beforeDelete}
}

type recoveryCore struct {
	typedcore.CoreV1Interface
	beforeDelete func(metav1.DeleteOptions) error
}

func (c recoveryCore) Pods(ns string) typedcore.PodInterface {
	return recoveryPods{c.CoreV1Interface.Pods(ns), c.beforeDelete}
}

type recoveryPods struct {
	typedcore.PodInterface
	beforeDelete func(metav1.DeleteOptions) error
}

func (p recoveryPods) Delete(ctx context.Context, name string, options metav1.DeleteOptions) error {
	if p.beforeDelete != nil {
		if err := p.beforeDelete(options); err != nil {
			return err
		}
	}
	return p.PodInterface.Delete(ctx, name, options)
}

type recoveryProvider struct{ client kubernetes.Interface }

func (p recoveryProvider) Client() kubernetes.Interface { return p.client }
func (p recoveryProvider) ClientForUser(string, []string) (kubernetes.Interface, error) {
	return p.client, nil
}
func (p recoveryProvider) ClientConfig() *rest.Config { return nil }

// Model API-server UID/RV guards and monotonically changing versions; the stock
// fake accepts stale version patches and deletes, which would hide this protocol.
func recoveryAPI(t *testing.T) (*fake.Clientset, *v1.Pod, func() *v1.Pod) {
	t.Helper()
	pod := createBatchPod()
	pod.Status.Phase = v1.PodPending
	pod.ResourceVersion = "1"
	client := fake.NewSimpleClientset(pod)
	gvr := v1.SchemeGroupVersion.WithResource("pods")
	live := func() *v1.Pod {
		obj, err := client.Tracker().Get(gvr, pod.Namespace, pod.Name)
		require.NoError(t, err)
		return obj.(*v1.Pod)
	}
	version := 1
	client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		var patch struct {
			Metadata struct {
				UID             string             `json:"uid"`
				ResourceVersion string             `json:"resourceVersion"`
				Annotations     map[string]*string `json:"annotations"`
			} `json:"metadata"`
		}
		require.NoError(t, json.Unmarshal(action.(clienttesting.PatchAction).GetPatch(), &patch))
		current := live()
		if patch.Metadata.UID != "" && patch.Metadata.UID != string(current.UID) || patch.Metadata.ResourceVersion != "" && patch.Metadata.ResourceVersion != current.ResourceVersion {
			return true, nil, apierrors.NewConflict(v1.Resource("pods"), pod.Name, fmt.Errorf("patch precondition"))
		}
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		for k, v := range patch.Metadata.Annotations {
			if v == nil {
				delete(current.Annotations, k)
			} else {
				current.Annotations[k] = *v
			}
		}
		version++
		current.ResourceVersion = strconv.Itoa(version)
		return true, current, client.Tracker().Update(gvr, current, pod.Namespace)
	})
	client.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		options := action.(clienttesting.DeleteAction).GetDeleteOptions()
		current := live()
		if options.Preconditions != nil {
			p := options.Preconditions
			if p.UID != nil && *p.UID != current.UID || p.ResourceVersion != nil && *p.ResourceVersion != current.ResourceVersion {
				return true, nil, apierrors.NewConflict(v1.Resource("pods"), pod.Name, fmt.Errorf("delete precondition"))
			}
		}
		now := metav1.Now()
		current.DeletionTimestamp = &now
		version++
		current.ResourceVersion = strconv.Itoa(version)
		return true, nil, client.Tracker().Update(gvr, current, pod.Namespace)
	})
	return client, pod, live
}

func recoveryContext(t *testing.T, client kubernetes.Interface, hook func(metav1.DeleteOptions) error) *KubernetesClusterContext {
	t.Helper()
	previous := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = previous })
	ctx := NewClusterContext(configuration.ApplicationConfiguration{ClusterId: "test", DeleteConcurrencyLimit: 1}, time.Minute, recoveryProvider{recoveryClient{client, hook}}, time.Minute)
	t.Cleanup(ctx.Stop)
	return ctx
}

func rejectRecoveryDelete(metav1.DeleteOptions) error {
	return apierrors.NewForbidden(v1.Resource("pods"), "test", fmt.Errorf("delete denied"))
}

func TestDeletionMarkerRecovery_ConcurrentAttempts(t *testing.T) {
	for _, successor := range []string{"conditional rejected", "conditional accepted", "cancellation"} {
		t.Run(successor, func(t *testing.T) {
			client, pod, live := recoveryAPI(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			first := recoveryContext(t, client, func(metav1.DeleteOptions) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-time.After(5 * time.Second):
					return fmt.Errorf("test delete barrier timed out")
				}
			})
			var hook func(metav1.DeleteOptions) error
			if successor == "conditional rejected" {
				hook = rejectRecoveryDelete
			}
			second := recoveryContext(t, client, hook)
			result := make(chan error, 1)
			go func() {
				result <- first.DeletePodWithCondition(pod, func(p *v1.Pod) bool { return p.Status.Phase == v1.PodPending }, true)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first delete did not start")
			}
			// Always release the blocked actor, including assertion failures.
			defer unblock()
			firstMarker := live().Annotations[domain.MarkedForDeletion]
			require.NotEmpty(t, firstMarker)
			if successor == "cancellation" {
				second.DeletePods([]*v1.Pod{pod})
				second.ProcessPodsToDelete()
			} else {
				err := second.DeletePodWithCondition(pod, func(p *v1.Pod) bool { return p.Status.Phase == v1.PodPending }, true)
				if successor == "conditional rejected" {
					require.True(t, apierrors.IsForbidden(err))
				} else {
					require.NoError(t, err)
				}
			}
			after := live()
			if successor == "conditional rejected" {
				assert.False(t, util.IsMarkedForDeletion(after), "both rejected conditional claims must relinquish reporting suppression")
			} else {
				assert.NotEqual(t, firstMarker, after.Annotations[domain.MarkedForDeletion], "adoption must be visible before DELETE")
				assert.True(t, util.IsMarkedForDeletion(after))
				assert.NotNil(t, after.DeletionTimestamp)
			}
			// Release before awaiting the first result (the deferred close is replaced).
			unblock()
			select {
			case err := <-result:
				require.True(t, apierrors.IsConflict(err), "the first conditional DELETE must be fenced by the successor version")
			case <-time.After(5 * time.Second):
				t.Fatal("first attempt did not finish")
			}
			assert.Equal(t, successor != "conditional rejected", util.IsMarkedForDeletion(live()))
		})
	}
}

func TestDeletionMarkerRecovery_PermanentIntentSurvivesConditionalSuccessors(t *testing.T) {
	client, pod, live := recoveryAPI(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	cancellation := recoveryContext(t, client, func(metav1.DeleteOptions) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("test barrier timed out")
		}
	})
	conditional := recoveryContext(t, client, rejectRecoveryDelete)
	done := make(chan struct{})
	go func() { cancellation.DeletePods([]*v1.Pod{pod}); cancellation.ProcessPodsToDelete(); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not start")
	}
	defer unblock()
	for i := 0; i < 2; i++ {
		before := live().Annotations[domain.MarkedForDeletion]
		err := conditional.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
		require.True(t, apierrors.IsForbidden(err))
		assert.True(t, util.IsMarkedForDeletion(live()))
		assert.NotEqual(t, before, live().Annotations[domain.MarkedForDeletion], "each adopter must renew without making cancellation intent removable")
	}
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not finish")
	}
	require.NotNil(t, live().DeletionTimestamp)
	require.True(t, util.IsMarkedForDeletion(live()))
}

func TestDeletionMarkerRecovery_CleanupFailuresAndChurn(t *testing.T) {
	for _, mode := range []string{"unrelated update", "cleanup forbidden", "cleanup conflict"} {
		t.Run(mode, func(t *testing.T) {
			client, pod, live := recoveryAPI(t)
			ctx := recoveryContext(t, client, rejectRecoveryDelete)
			removals := 0
			client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				var patch map[string]interface{}
				require.NoError(t, json.Unmarshal(action.(clienttesting.PatchAction).GetPatch(), &patch))
				annotations := patch["metadata"].(map[string]interface{})["annotations"].(map[string]interface{})
				if value, ok := annotations[domain.MarkedForDeletion]; !ok || value != nil {
					return false, nil, nil
				}
				removals++
				if mode == "cleanup forbidden" {
					return true, nil, apierrors.NewForbidden(v1.Resource("pods"), pod.Name, fmt.Errorf("cleanup denied"))
				}
				if mode == "cleanup conflict" || removals == 1 {
					return true, nil, apierrors.NewConflict(v1.Resource("pods"), pod.Name, fmt.Errorf("cleanup churn"))
				}
				return false, nil, nil
			})
			err := ctx.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
			require.Error(t, err)
			if mode == "unrelated update" {
				require.False(t, util.IsMarkedForDeletion(live()))
				require.Equal(t, 2, removals)
			} else {
				require.True(t, util.IsMarkedForDeletion(live()))
				require.ErrorContains(t, err, "remove abandoned deletion marker")
				if mode == "cleanup conflict" {
					require.Equal(t, 3, removals)
				}
			}
		})
	}
}

func TestDeletionMarkerRecovery_AmbiguousPredecessorIsFenced(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%t", committed), func(t *testing.T) {
			client, pod, live := recoveryAPI(t)
			var originalOptions metav1.DeleteOptions
			first := recoveryContext(t, client, func(options metav1.DeleteOptions) error {
				originalOptions = options
				if committed {
					require.NoError(t, client.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, options))
				}
				return apierrors.NewTimeoutError("response lost", 0)
			})
			err := first.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
			require.True(t, apierrors.IsTimeout(err))
			require.True(t, util.IsMarkedForDeletion(live()))
			second := recoveryContext(t, client, rejectRecoveryDelete)
			err = second.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
			if committed {
				require.NoError(t, err)
				require.True(t, util.IsMarkedForDeletion(live()))
				require.NotNil(t, live().DeletionTimestamp)
			} else {
				require.True(t, apierrors.IsForbidden(err))
				require.False(t, util.IsMarkedForDeletion(live()))
				err = client.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, originalOptions)
				require.True(t, apierrors.IsConflict(err), "a delayed earlier DELETE must not commit after cleanup")
			}
		})
	}
}

func TestDeletionMarkerRecovery_RefreshesStaleMarkedCache(t *testing.T) {
	client, pod, live := recoveryAPI(t)
	ctx := recoveryContext(t, client, rejectRecoveryDelete)
	var stale *v1.Pod
	ctx.kubernetesClient = recoveryClient{client, func(metav1.DeleteOptions) error { stale = live(); return rejectRecoveryDelete(metav1.DeleteOptions{}) }}
	err := ctx.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
	require.True(t, apierrors.IsForbidden(err))
	require.False(t, util.IsMarkedForDeletion(live()))
	require.NotNil(t, stale)
	require.NoError(t, ctx.podInformer.Informer().GetStore().Update(stale))
	err = ctx.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
	require.True(t, apierrors.IsForbidden(err))
	require.False(t, util.IsMarkedForDeletion(live()))
}

func TestDeletionMarkerRecovery_CleanupReadAndOwnershipChanges(t *testing.T) {
	for _, change := range []string{"read failure", "replacement", "cancellation"} {
		t.Run(change, func(t *testing.T) {
			client, pod, live := recoveryAPI(t)
			rejected := false
			ctx := recoveryContext(t, client, func(metav1.DeleteOptions) error { rejected = true; return rejectRecoveryDelete(metav1.DeleteOptions{}) })
			client.PrependReactor("get", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
				if !rejected {
					return false, nil, nil
				}
				if change == "read failure" {
					return true, nil, fmt.Errorf("cleanup GET unavailable")
				}
				current := live()
				if change == "replacement" {
					current.UID = "replacement"
				} else {
					current.Annotations[domain.MarkedForDeletion] = "cancellation"
				}
				require.NoError(t, client.Tracker().Update(v1.SchemeGroupVersion.WithResource("pods"), current, pod.Namespace))
				return true, current, nil
			})
			err := ctx.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
			require.True(t, apierrors.IsForbidden(err))
			require.True(t, util.IsMarkedForDeletion(live()))
			if change == "read failure" {
				require.ErrorContains(t, err, "cleanup GET unavailable")
				require.ErrorContains(t, err, "remove abandoned deletion marker")
			}
		})
	}
}

func TestDeletionMarkerRecovery_CleanupCASProtectsConcurrentAdoption(t *testing.T) {
	client, pod, live := recoveryAPI(t)
	ctx := recoveryContext(t, client, rejectRecoveryDelete)
	removalAttempted := false
	client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		var patch struct {
			Metadata struct {
				Annotations map[string]*string `json:"annotations"`
			} `json:"metadata"`
		}
		require.NoError(t, json.Unmarshal(action.(clienttesting.PatchAction).GetPatch(), &patch))
		value, ok := patch.Metadata.Annotations[domain.MarkedForDeletion]
		if !ok || value != nil {
			return false, nil, nil
		}
		require.False(t, removalAttempted)
		removalAttempted = true
		current := live()
		current.Annotations[domain.MarkedForDeletion] = "concurrent-cancellation"
		current.ResourceVersion = "adopted-version"
		require.NoError(t, client.Tracker().Update(v1.SchemeGroupVersion.WithResource("pods"), current, pod.Namespace))
		return false, nil, nil
	})
	err := ctx.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
	require.True(t, apierrors.IsForbidden(err))
	require.True(t, removalAttempted)
	require.Equal(t, "concurrent-cancellation", live().Annotations[domain.MarkedForDeletion])
}

func TestDeletionMarkerRecovery_UnknownMarkersRemainPermanent(t *testing.T) {
	for _, marker := range []string{"legacy", "conditional:not-a-uuid"} {
		t.Run(marker, func(t *testing.T) {
			client, pod, live := recoveryAPI(t)
			current := live()
			current.Annotations = map[string]string{domain.MarkedForDeletion: marker, "other": "preserved"}
			require.NoError(t, client.Tracker().Update(v1.SchemeGroupVersion.WithResource("pods"), current, pod.Namespace))
			ctx := recoveryContext(t, client, rejectRecoveryDelete)
			for i := 0; i < 2; i++ {
				err := ctx.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
				require.True(t, apierrors.IsForbidden(err))
				require.True(t, util.IsMarkedForDeletion(live()))
				require.Equal(t, "preserved", live().Annotations["other"])
			}
		})
	}
}

func TestDeletionMarkerRecovery_NonPessimisticClaims(t *testing.T) {
	for _, preexisting := range []bool{false, true} {
		t.Run(fmt.Sprintf("preexisting=%t", preexisting), func(t *testing.T) {
			client, pod, live := recoveryAPI(t)
			if preexisting {
				current := live()
				current.Annotations = map[string]string{domain.MarkedForDeletion: "legacy"}
				require.NoError(t, client.Tracker().Update(v1.SchemeGroupVersion.WithResource("pods"), current, pod.Namespace))
			}
			ctx := recoveryContext(t, client, rejectRecoveryDelete)
			err := ctx.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, false)
			require.True(t, apierrors.IsForbidden(err))
			require.Equal(t, preexisting, util.IsMarkedForDeletion(live()))
		})
	}
	t.Run("outstanding unconditional delete", func(t *testing.T) {
		client, pod, live := recoveryAPI(t)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		first := recoveryContext(t, client, func(options metav1.DeleteOptions) error {
			require.NotNil(t, options.Preconditions.UID)
			require.Nil(t, options.Preconditions.ResourceVersion)
			close(entered)
			select {
			case <-release:
				return nil
			case <-time.After(5 * time.Second):
				return fmt.Errorf("test barrier timed out")
			}
		})
		successor := recoveryContext(t, client, rejectRecoveryDelete)
		done := make(chan error, 1)
		go func() { done <- first.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, false) }()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("nonpessimistic delete did not start")
		}
		err := successor.DeletePodWithCondition(pod, func(*v1.Pod) bool { return true }, true)
		require.True(t, apierrors.IsForbidden(err))
		require.True(t, util.IsMarkedForDeletion(live()))
		unblock()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("nonpessimistic delete did not finish")
		}
		require.NotNil(t, live().DeletionTimestamp)
		require.True(t, util.IsMarkedForDeletion(live()))
	})
}
