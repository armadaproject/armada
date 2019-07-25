package service

import (
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestGetAllAvailableProcessingNodes_ShouldReturnAvailableProcessingNodes(t *testing.T) {
	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
			Taints:        nil,
		},
	}

	nodes := []*v1.Node{&node}
	result := getAllAvailableProcessingNodes(nodes)

	assert.Equal(t, len(result), 1)
}

func TestGetAllAvailableProcessingNodes_ShouldFilterUnschedulableNodes(t *testing.T) {
	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: true,
			Taints:        nil,
		},
	}

	nodes := []*v1.Node{&node}
	result := getAllAvailableProcessingNodes(nodes)

	assert.Equal(t, len(result), 0)
}

func TestGetAllAvailableProcessingNodes_ShouldFilterNodesWithNoScheduleTaint(t *testing.T) {
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
	result := getAllAvailableProcessingNodes(nodes)

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

func TestCalculateTotalResource(t *testing.T) {
	resources := makeDefaultResource()
	node1 := makeNodeWithResource(resources)
	node2 := makeNodeWithResource(resources)

	//Expected is resources * 2 nodes
	expectedResult := common.FromResourceList(resources)
	expectedResult.Add(expectedResult)

	result := calculateTotalResource([]*v1.Node{&node1, &node2})
	assert.Equal(t, result, expectedResult)
}

func TestCalculateTotalResourceLimit_ShouldSumAllContainers(t *testing.T) {
	resources := makeDefaultResource()
	pod := makePodWthResource([]*v1.ResourceList{&resources, &resources})

	//Expected is resources * 2 containers
	expectedResult := common.FromResourceList(resources)
	expectedResult.Add(expectedResult)

	result := calculateTotalResourceLimit([]*v1.Pod{&pod})
	assert.Equal(t, result, expectedResult)
}

func TestCalculateTotalResourceLimit_ShouldSumAllPods(t *testing.T) {
	resources := makeDefaultResource()
	pod1 := makePodWthResource([]*v1.ResourceList{&resources})
	pod2 := makePodWthResource([]*v1.ResourceList{&resources})

	//Expected is resources * 2 pods
	expectedResult := common.FromResourceList(resources)
	expectedResult.Add(expectedResult)

	result := calculateTotalResourceLimit([]*v1.Pod{&pod1, &pod2})
	assert.Equal(t, result, expectedResult)
}

func makeDefaultResource() v1.ResourceList {
	cpuResource := resource.NewMilliQuantity(100, resource.DecimalSI)
	memoryResource := resource.NewMilliQuantity(50*1024*1024*1024, resource.DecimalSI)
	storageResource := resource.NewMilliQuantity(500*1024*1024*1024, resource.DecimalSI)
	ephemeralStorageResource := resource.NewMilliQuantity(20*1024*1024*1024, resource.DecimalSI)
	resourceMap := map[v1.ResourceName]resource.Quantity{
		v1.ResourceCPU:              *cpuResource,
		v1.ResourceMemory:           *memoryResource,
		v1.ResourceStorage:          *storageResource,
		v1.ResourceEphemeralStorage: *ephemeralStorageResource,
	}
	return resourceMap
}

func makePodWthResource(resources []*v1.ResourceList) v1.Pod {
	containers := make([]v1.Container, len(resources))
	for i, res := range resources {
		containers[i] = v1.Container{
			Resources: v1.ResourceRequirements{
				Limits: *res,
			},
		}
	}
	pod := v1.Pod{
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}

	return pod
}

func makeNodeWithResource(resources map[v1.ResourceName]resource.Quantity) v1.Node {
	node := v1.Node{
		Status: v1.NodeStatus{
			Allocatable: resources,
		},
	}
	return node
}

func TestFilterCompletedPods(t *testing.T) {
	runningPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	completedPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	result := filterCompletedPods([]*v1.Pod{&runningPod, &completedPod})

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], &completedPod)
}

func TestFilterCompletedPods_ShouldReturnEmptyIfNoCompletedPods(t *testing.T) {
	runningPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	pendingPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	result := filterCompletedPods([]*v1.Pod{&runningPod, &pendingPod})

	assert.Equal(t, len(result), 0)
}
