package service

import (
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

func TestCalculateTotalCpu(t *testing.T) {
	resources := resource.NewMilliQuantity(100, resource.DecimalSI)
	resourceMap := map[v1.ResourceName]resource.Quantity{v1.ResourceCPU: *resources}
	node1 := makeNodeWithResource(resourceMap)
	node2 := makeNodeWithResource(resourceMap)

	result := calculateTotalCpu([]*v1.Node{&node1, &node2})

	//Expected is resources *2
	resources.Add(*resources)
	assert.Equal(t, result, *resources)
}

func TestCalculateTotalMemory(t *testing.T) {
	resources := resource.NewMilliQuantity(50*1024*1024, resource.DecimalSI)
	resourceMap := map[v1.ResourceName]resource.Quantity{v1.ResourceMemory: *resources}
	node1 := makeNodeWithResource(resourceMap)
	node2 := makeNodeWithResource(resourceMap)

	result := calculateTotalMemory([]*v1.Node{&node1, &node2})

	//Expected is resources *2
	resources.Add(*resources)
	assert.Equal(t, result, *resources)
}

func TestCalculateTotalCpuLimit_ShouldSumAllContainers(t *testing.T) {
	resources := resource.NewMilliQuantity(100, resource.DecimalSI)
	pod := makePodWthResource(v1.ResourceCPU, []*resource.Quantity{resources, resources})

	result := calculateTotalCpuLimit([]*v1.Pod{&pod})

	//Expected is resources * 2 containers
	resources.Add(*resources)
	assert.Equal(t, result, *resources)
}

func TestCalculateTotalCpuLimit_ShouldSumAllPods(t *testing.T) {
	resources := resource.NewMilliQuantity(100, resource.DecimalSI)
	pod1 := makePodWthResource(v1.ResourceCPU, []*resource.Quantity{resources})
	pod2 := makePodWthResource(v1.ResourceCPU, []*resource.Quantity{resources})

	result := calculateTotalCpuLimit([]*v1.Pod{&pod1, &pod2})

	//Expected is resources * 2 pods
	resources.Add(*resources)
	assert.Equal(t, result, *resources)
}

func TestCalculateTotalMemoryLimit_ShouldSumAllContainers(t *testing.T) {
	resources := resource.NewMilliQuantity(50*1024*1024, resource.DecimalSI)
	pod := makePodWthResource(v1.ResourceMemory, []*resource.Quantity{resources, resources})

	result := calculateTotalMemoryLimit([]*v1.Pod{&pod})

	//Expected is resources * 2 containers
	resources.Add(*resources)
	assert.Equal(t, result, *resources)
}

func TestCalculateTotalMemoryLimit__ShouldSumAllPods(t *testing.T) {
	resources := resource.NewMilliQuantity(50*1024*1024, resource.DecimalSI)
	pod := makePodWthResource(v1.ResourceMemory, []*resource.Quantity{resources, resources})

	result := calculateTotalMemoryLimit([]*v1.Pod{&pod})

	//Expected is resources * 2 containers
	resources.Add(*resources)
	assert.Equal(t, result, *resources)
}

func makePodWthResource(resourceName v1.ResourceName, resources []*resource.Quantity) v1.Pod {
	containers := make([]v1.Container, len(resources))
	for i, res := range resources {
		containers[i] = v1.Container{
			Resources: v1.ResourceRequirements{
				Limits: map[v1.ResourceName]resource.Quantity{resourceName: *res},
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
