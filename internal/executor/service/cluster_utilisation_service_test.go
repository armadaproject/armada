package service

import (
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestFilterAvailableProcessingNodes_ShouldReturnAvailableProcessingNodes(t *testing.T) {
	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
			Taints:        nil,
		},
	}

	nodes := []*v1.Node{&node}
	result := filterAvailableProcessingNodes(nodes)

	assert.Equal(t, len(result), 1)
}

func TestFilterAvailableProcessingNodes_ShouldFilterUnschedulableNodes(t *testing.T) {
	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: true,
			Taints:        nil,
		},
	}

	nodes := []*v1.Node{&node}
	result := filterAvailableProcessingNodes(nodes)

	assert.Equal(t, len(result), 0)
}

func TestFilterAvailableProcessingNodes_ShouldFilterNodesWithNoScheduleTaint(t *testing.T) {
	taint := v1.Taint{
		Effect: v1.TaintEffectNoSchedule,
	}
	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
			Taints:        []v1.Taint{taint},
		},
	}

	nodes := []*v1.Node{&node}
	result := filterAvailableProcessingNodes(nodes)

	assert.Equal(t, len(result), 0)
}

func TestGetAllPodsOnNodes_ShouldExcludePodsNoOnGivenNodes(t *testing.T) {
	presentNodeName := "Node1"
	podOnNode := v1.Pod{
		Spec: v1.PodSpec{
			NodeName: presentNodeName,
		},
	}
	podNotOnNode := v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "Node2",
		},
	}

	node := v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: presentNodeName,
		},
	}
	pods := []*v1.Pod{&podOnNode, &podNotOnNode}
	nodes := []*v1.Node{&node}

	result := getAllPodsOnNodes(pods, nodes)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Spec.NodeName, presentNodeName)
}

func TestGetAllPodsOnNodes_ShouldHandleNoNodesProvided(t *testing.T) {
	podOnNode := v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "Node1",
		},
	}

	pods := []*v1.Pod{&podOnNode}
	var nodes []*v1.Node

	result := getAllPodsOnNodes(pods, nodes)

	assert.Equal(t, len(result), 0)
}

func TestGetUsageByQueue_HasAnEntryPerQueue(t *testing.T) {
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", &podResource)
	queue1Pod2 := makePodWithResource("queue1", &podResource)
	queue2Pod1 := makePodWithResource("queue2", &podResource)

	pods := []*v1.Pod{&queue1Pod1, &queue1Pod2, &queue2Pod1}

	result := getUsageByQueue(pods)
	assert.Equal(t, len(result), 2)
	assert.True(t, hasKey(result, "queue1"))
	assert.True(t, hasKey(result, "queue2"))
}

func TestGetUsageByQueue_SkipsPodsWithoutQueue(t *testing.T) {
	podResource := makeResourceList(2, 50)
	pod := makePodWithResource("", &podResource)
	pod.Labels = make(map[string]string)

	result := getUsageByQueue([]*v1.Pod{&pod})
	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func TestGetUsageByQueue_AggregatesPodResourcesInAQueue(t *testing.T) {
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", &podResource)
	queue1Pod2 := makePodWithResource("queue1", &podResource)

	pods := []*v1.Pod{&queue1Pod1, &queue1Pod2}

	expectedResource := makeResourceList(4, 100)
	expectedResult := map[string]common.ComputeResources{"queue1": common.FromResourceList(expectedResource)}

	result := getUsageByQueue(pods)
	assert.Equal(t, result, expectedResult)
}

func TestGetUsageByQueue_HandlesEmptyList(t *testing.T) {
	var pods []*v1.Pod

	result := getUsageByQueue(pods)

	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func hasKey(value map[string]common.ComputeResources, key string) bool {
	_, ok := value[key]
	return ok
}

func makeResourceList(cores int64, gigabytesRam int64) v1.ResourceList {
	cpuResource := resource.NewQuantity(cores, resource.DecimalSI)
	memoryResource := resource.NewQuantity(gigabytesRam*1024*1024*1024, resource.DecimalSI)
	resourceMap := map[v1.ResourceName]resource.Quantity{
		v1.ResourceCPU:    *cpuResource,
		v1.ResourceMemory: *memoryResource,
	}
	return resourceMap
}

func makePodWithResource(queue string, resource *v1.ResourceList) v1.Pod {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{domain.Queue: queue},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits: *resource,
					},
				},
			},
		},
	}
	return pod
}
