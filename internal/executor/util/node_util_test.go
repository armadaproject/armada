package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFilterNodes(t *testing.T) {
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}
	node2 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
		},
	}

	result := FilterNodes([]*v1.Node{node1, node2}, func(node *v1.Node) bool {
		return node.Name == "node1"
	})

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], node1)
}

func TestFilterNodes_WhenNoPodsMatchFilter(t *testing.T) {
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}
	node2 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
		},
	}

	result := FilterNodes([]*v1.Node{node1, node2}, func(node *v1.Node) bool {
		return node.Name == "node3"
	})

	assert.Equal(t, len(result), 0)
}

func TestGetPodsOnNodes(t *testing.T) {
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}

	pod1 := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: node1.Name,
		},
	}
	pod2 := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "node2",
		},
	}
	pod3 := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "",
		},
	}

	result := GetPodsOnNodes([]*v1.Pod{pod1, pod2, pod3}, []*v1.Node{node1})
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], pod1)
}

func TestGetPodsOnNodes_NoMatches(t *testing.T) {
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}

	pod1 := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "pod1",
		},
	}
	result := GetPodsOnNodes([]*v1.Pod{pod1}, []*v1.Node{node1})
	assert.Equal(t, len(result), 0)
}

func TestGetPodsOnNodes_Empty(t *testing.T) {
	node1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}

	pod1 := &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "pod1",
		},
	}
	result := GetPodsOnNodes([]*v1.Pod{}, []*v1.Node{node1})
	assert.Equal(t, len(result), 0)

	result = GetPodsOnNodes([]*v1.Pod{pod1}, []*v1.Node{})
	assert.Equal(t, len(result), 0)

	result = GetPodsOnNodes([]*v1.Pod{}, []*v1.Node{})
	assert.Equal(t, len(result), 0)
}

func TestExtractNodeNames(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
	node2 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2"}}

	result := ExtractNodeNames([]*v1.Node{})
	assert.Equal(t, []string{}, result)

	result = ExtractNodeNames([]*v1.Node{node1, node2})
	assert.Equal(t, []string{node1.Name, node2.Name}, result)

	// Returns duplicates
	result = ExtractNodeNames([]*v1.Node{node1, node1})
	assert.Equal(t, []string{node1.Name, node1.Name}, result)
}

func TestMergeNodeList(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
	node2 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2"}}

	result := MergeNodeList([]*v1.Node{node1}, []*v1.Node{node2})
	assert.Equal(t, []*v1.Node{node1, node2}, result)
}

func TestMergeNodeList_DoesNotAddDuplicateNodes(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}

	result := MergeNodeList([]*v1.Node{node1}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{node1}, result)
}

func TestMergeNodeList_HandlesEmptyLists(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}

	result := MergeNodeList([]*v1.Node{}, []*v1.Node{})
	assert.Equal(t, []*v1.Node{}, result)

	result = MergeNodeList([]*v1.Node{}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{node1}, result)

	result = MergeNodeList([]*v1.Node{node1}, []*v1.Node{})
	assert.Equal(t, []*v1.Node{node1}, result)
}

func TestRemoveNodesFromList(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
	node2 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node2"}}

	result := RemoveNodesFromList([]*v1.Node{node1, node2}, []*v1.Node{node2})
	assert.Equal(t, []*v1.Node{node1}, result)
}

func TestRemoveNodesFromList_HandlesEmptyLists(t *testing.T) {
	node1 := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}

	result := RemoveNodesFromList([]*v1.Node{}, []*v1.Node{})
	assert.Equal(t, []*v1.Node{}, result)

	result = RemoveNodesFromList([]*v1.Node{}, []*v1.Node{node1})
	assert.Equal(t, []*v1.Node{}, result)

	result = RemoveNodesFromList([]*v1.Node{node1}, []*v1.Node{})
	assert.Equal(t, []*v1.Node{node1}, result)
}

func TestIsReady(t *testing.T) {
	readyNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "ready"}, Status: v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:   v1.NodeReady,
				Status: v1.ConditionTrue,
			},
		},
	}}
	assert.True(t, IsReady(readyNode))
}

func TestIsReady_NotReadyNodes(t *testing.T) {
	notReadyNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "notReady"}, Status: v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:   v1.NodeReady,
				Status: v1.ConditionFalse,
			},
		},
	}}
	unknownReadyStateNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "unknownReady"}, Status: v1.NodeStatus{
		Conditions: []v1.NodeCondition{
			{
				Type:   v1.NodeReady,
				Status: v1.ConditionFalse,
			},
		},
	}}
	missingReadyStateNode := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "missingReady"}}

	assert.False(t, IsReady(notReadyNode))
	assert.False(t, IsReady(unknownReadyStateNode))
	assert.False(t, IsReady(missingReadyStateNode))
}
