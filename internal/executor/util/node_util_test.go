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
