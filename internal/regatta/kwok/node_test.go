package kwok

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

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
	t.Run("a node regatta did not create is an error too", func(t *testing.T) {
		client := fake.NewSimpleClientset(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "kwok-node-gpu-0"}})
		err := ApplyFakeNodes(ctx, client, profile, 1, "mine", 4)
		require.ErrorContains(t, err, "was not created by regatta")
	})
}
