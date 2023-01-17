package utilisation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/domain"
)

func TestGetNodesHostingActiveManagedPods(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
	pendingPod := createPod(v1.PodPending, node1.Name, true)
	runningPod := createPod(v1.PodRunning, node1.Name, true)

	result := getNodesHostingActiveManagedPods([]*v1.Pod{pendingPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{node1}, result)

	result = getNodesHostingActiveManagedPods([]*v1.Pod{runningPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{node1}, result)
}

func TestGetNodesHostingActiveManagedPods_IgnoresUnmanagedPods(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
	pod1 := createPod(v1.PodRunning, node1.Name, false)

	result := getNodesHostingActiveManagedPods([]*v1.Pod{pod1}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)
}

func TestGetNodesHostingActiveManagedPods_IgnoresPodsNotPresentOnGivenNode(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
	pod1 := createPod(v1.PodRunning, "node2", true)

	result := getNodesHostingActiveManagedPods([]*v1.Pod{pod1}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)
}

func TestGetNodesHostingActiveManagedPods_HandlesTerminatedPods(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
	now := metav1.NewTime(time.Now())

	recentlyDeletedPod := createPod(v1.PodRunning, node1.Name, true)
	recentlyDeletedPod.DeletionTimestamp = &now
	result := getNodesHostingActiveManagedPods([]*v1.Pod{recentlyDeletedPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{node1}, result)

	olderThanGracePeriod := metav1.NewTime(time.Now().Add(-inactivePodGracePeriod).Add(-1 * time.Minute))
	stuckInTerminatingPod := createPod(v1.PodRunning, node1.Name, true)
	stuckInTerminatingPod.DeletionTimestamp = &olderThanGracePeriod
	result = getNodesHostingActiveManagedPods([]*v1.Pod{stuckInTerminatingPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)
}

func TestGetNodesHostingActiveManagedPods_HandlesTerminatingPods(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}

	recentlySucceededPod := createPod(v1.PodSucceeded, node1.Name, true)
	recentlySucceededPod.Status.Conditions = []v1.PodCondition{{LastTransitionTime: metav1.NewTime(time.Now())}}
	result := getNodesHostingActiveManagedPods([]*v1.Pod{recentlySucceededPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{node1}, result)

	olderThanGracePeriod := metav1.NewTime(time.Now().Add(-inactivePodGracePeriod).Add(-1 * time.Minute))
	oldSucceededPod := createPod(v1.PodSucceeded, node1.Name, true)
	oldSucceededPod.Status.Conditions = []v1.PodCondition{{LastTransitionTime: olderThanGracePeriod}}
	result = getNodesHostingActiveManagedPods([]*v1.Pod{oldSucceededPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)

	unknownAgeSucceededPod := createPod(v1.PodSucceeded, node1.Name, true)
	result = getNodesHostingActiveManagedPods([]*v1.Pod{unknownAgeSucceededPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)

	recentlyFailedPod := createPod(v1.PodFailed, node1.Name, true)
	recentlyFailedPod.Status.Conditions = []v1.PodCondition{{LastTransitionTime: metav1.NewTime(time.Now())}}
	result = getNodesHostingActiveManagedPods([]*v1.Pod{recentlyFailedPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{node1}, result)

	olderThanGracePeriod = metav1.NewTime(time.Now().Add(-inactivePodGracePeriod).Add(-1 * time.Minute))
	oldFailedPod := createPod(v1.PodFailed, node1.Name, true)
	oldFailedPod.Status.Conditions = []v1.PodCondition{{LastTransitionTime: olderThanGracePeriod}}
	result = getNodesHostingActiveManagedPods([]*v1.Pod{oldFailedPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)

	unknownAgeFailedPod := createPod(v1.PodFailed, node1.Name, true)
	result = getNodesHostingActiveManagedPods([]*v1.Pod{unknownAgeFailedPod}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)
}

func TestGetNodesHostingActiveManagedPods_HandlesEmptyInputs(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
	pod1 := createPod(v1.PodRunning, node1.Name, true)

	result := getNodesHostingActiveManagedPods([]*v1.Pod{}, []*v1.Node{})
	assert.Equal(t, []*v1.Node{}, result)

	result = getNodesHostingActiveManagedPods([]*v1.Pod{pod1}, []*v1.Node{})
	assert.Equal(t, []*v1.Node{}, result)

	result = getNodesHostingActiveManagedPods([]*v1.Pod{}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)
}

func createPod(phase v1.PodPhase, nodeName string, isManaged bool) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "name" + util2.NewULID(),
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
		},
	}
	if isManaged {
		pod.Labels = map[string]string{
			domain.JobId: "jobid" + util2.NewULID(),
		}
	}
	return pod
}
