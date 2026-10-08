package kwok

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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
