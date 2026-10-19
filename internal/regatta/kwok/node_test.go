package kwok

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"

	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
)

func fakeNode(name, target string, ready bool) *v1.Node {
	status := v1.ConditionFalse
	if ready {
		status = v1.ConditionTrue
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{NodeAnnotation: NodeAnnotationOK, TargetLabel: target}},
		Status:     v1.NodeStatus{Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: status}}},
	}
}

func TestWaitUntilReady(t *testing.T) {
	previous := readyPollInterval
	readyPollInterval = 5 * time.Millisecond
	t.Cleanup(func() { readyPollInterval = previous })

	t.Run("another target's nodes that are not Ready do not hold this target up", func(t *testing.T) {
		client := fake.NewSimpleClientset(
			fakeNode("mine-1", "mine", true), fakeNode("mine-2", "mine", true),
			fakeNode("theirs-1", "theirs", false),
		)
		require.NoError(t, WaitUntilReady(context.Background(), client, "mine", time.Second))
	})
	t.Run("this target's own node that is not Ready does", func(t *testing.T) {
		client := fake.NewSimpleClientset(fakeNode("mine-1", "mine", true), fakeNode("mine-2", "mine", false))
		err := WaitUntilReady(context.Background(), client, "mine", 100*time.Millisecond)
		require.ErrorContains(t, err, "timed out")
	})
	t.Run("a target with no nodes is not ready", func(t *testing.T) {
		client := fake.NewSimpleClientset(fakeNode("theirs-1", "theirs", true))
		err := WaitUntilReady(context.Background(), client, "mine", 100*time.Millisecond)
		require.ErrorContains(t, err, "no fake nodes found")
	})
	t.Run("a cancelled context ends the wait at once", func(t *testing.T) {
		client := fake.NewSimpleClientset(fakeNode("mine-1", "mine", false))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		start := time.Now()
		err := WaitUntilReady(ctx, client, "mine", time.Minute)
		require.ErrorIs(t, err, context.Canceled)
		require.Less(t, time.Since(start), 5*time.Second)
	})
}

func TestApplyFakeNodes_ARerunIsFineButAnotherTargetsNodesAreNotAdopted(t *testing.T) {
	profile := &regattaconfig.NodeProfile{Name: "gpu"}
	ctx := context.Background()

	t.Run("a rerun by the same target finds its own nodes", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		require.NoError(t, ApplyFakeNodes(ctx, client, profile, 3, "mine", 4))
		require.NoError(t, ApplyFakeNodes(ctx, client, profile, 3, "mine", 4))
		nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		require.NoError(t, err)
		require.Len(t, nodes.Items, 3)
	})
	t.Run("a node another target created is an error, not silently reused", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		require.NoError(t, ApplyFakeNodes(ctx, client, profile, 2, "theirs", 4))
		err := ApplyFakeNodes(ctx, client, profile, 2, "mine", 4)
		require.ErrorContains(t, err, `belongs to target "theirs", not "mine"`)
	})
	t.Run("nodes left with different resources are an error, not silently reused", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		old := &regattaconfig.NodeProfile{Name: "gpu", Allocatable: v1.ResourceList{v1.ResourceCPU: resource.MustParse("8")}}
		changed := &regattaconfig.NodeProfile{Name: "gpu", Allocatable: v1.ResourceList{v1.ResourceCPU: resource.MustParse("64")}}
		require.NoError(t, ApplyFakeNodes(ctx, client, old, 2, "mine", 4))
		require.NoError(t, ApplyFakeNodes(ctx, client, old, 2, "mine", 4), "the same profile is still a clean rerun")
		err := ApplyFakeNodes(ctx, client, changed, 2, "mine", 4)
		require.ErrorContains(t, err, "different profile")
		require.ErrorContains(t, err, "tear target")
	})
	t.Run("nodes left over from a larger count are an error", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		require.NoError(t, ApplyFakeNodes(ctx, client, profile, 5, "mine", 4))
		err := ApplyFakeNodes(ctx, client, profile, 2, "mine", 4)
		require.ErrorContains(t, err, "3 more")
	})
	t.Run("a profile whose name extends another's is not counted as its surplus", func(t *testing.T) {
		client := fake.NewSimpleClientset()
		require.NoError(t, ApplyFakeNodes(ctx, client, &regattaconfig.NodeProfile{Name: "gpu-big"}, 4, "mine", 4))
		require.NoError(t, ApplyFakeNodes(ctx, client, profile, 2, "mine", 4))
	})
	t.Run("a node regatta did not create is an error too", func(t *testing.T) {
		client := fake.NewSimpleClientset(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kwok-node-gpu-0"}})
		err := ApplyFakeNodes(ctx, client, profile, 1, "mine", 4)
		require.ErrorContains(t, err, "was not created by regatta")
	})
}

func TestWaitUntilReady_AListThatNeverAnswersEndsAtTheDeadline(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select { // an API server that accepts the request and never replies
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); server.Close() })
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)

	start := time.Now()
	err = WaitUntilReady(context.Background(), client, "mine", 200*time.Millisecond)

	require.ErrorContains(t, err, "timed out waiting for fake nodes")
	require.Less(t, time.Since(start), 5*time.Second, "the in-flight request was cancelled at the deadline")
}
