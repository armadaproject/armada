package utilisation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/common"
	util2 "github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/domain"
	fakeContext "github.com/G-Research/armada/internal/executor/fake/context"
)

var testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}

func TestFilterAvailableProcessingNodes_ShouldReturnAvailableProcessingNodes(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, nil)
	service := NewClusterUtilisationService(context, nil, nil, nil, nil)

	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
			Taints:        nil,
		},
	}

	nodes := []*v1.Node{&node}
	result := service.filterAvailableProcessingNodes(nodes)

	assert.Equal(t, len(result), 1)
}

func TestFilterAvailableProcessingNodes_ShouldFilterUnschedulableNodes(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, nil)
	service := NewClusterUtilisationService(context, nil, nil, nil, nil)

	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: true,
			Taints:        nil,
		},
	}

	nodes := []*v1.Node{&node}
	result := service.filterAvailableProcessingNodes(nodes)

	assert.Equal(t, len(result), 0)
}

func TestFilterAvailableProcessingNodes_ShouldFilterNodesWithNoScheduleTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, nil)
	service := NewClusterUtilisationService(context, nil, nil, nil, nil)

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
	result := service.filterAvailableProcessingNodes(nodes)

	assert.Equal(t, len(result), 0)
}

func TestGetAllPodsUsingResourceOnProcessingNodes_ShouldExcludePodsNotOnGivenNodes(t *testing.T) {
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

	result := getAllPodsRequiringResourceOnProcessingNodes(pods, nodes)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Spec.NodeName, presentNodeName)
}

func TestGetAllPodsUsingResourceOnProcessingNodes_ShouldHandleNoNodesProvided(t *testing.T) {
	podOnNode := v1.Pod{
		Spec: v1.PodSpec{
			NodeName: "Node1",
		},
	}

	pods := []*v1.Pod{&podOnNode}
	var nodes []*v1.Node

	result := getAllPodsRequiringResourceOnProcessingNodes(pods, nodes)

	assert.Equal(t, len(result), 0)
}

func TestGetAllPodsUsingResourceOnProcessingNodes_ShouldIncludeManagedPodsOnNodes(t *testing.T) {
	podOnNode := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{domain.JobId: "label"},
		},
		Spec: v1.PodSpec{
			NodeName: "Node1",
		},
	}
	node := v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Node1",
		},
	}

	pods := []*v1.Pod{&podOnNode}
	nodes := []*v1.Node{&node}

	result := getAllPodsRequiringResourceOnProcessingNodes(pods, nodes)

	assert.Equal(t, len(result), 1)
}

func TestGetAllPodsUsingResourceOnProcessingNodes_ShouldIncludeManagedPodNotAssignedToAnyNode(t *testing.T) {
	podOnNode := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{domain.JobId: "label"},
		},
		Spec: v1.PodSpec{
			NodeName: "",
		},
	}
	node := v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Node1",
		},
	}

	pods := []*v1.Pod{&podOnNode}
	nodes := []*v1.Node{&node}

	result := getAllPodsRequiringResourceOnProcessingNodes(pods, nodes)

	assert.Equal(t, len(result), 1)
}

func TestGetAllPodsUsingResourceOnProcessingNodes_ShouldExcludeManagedPodNotAssignedToGivenNodes(t *testing.T) {
	podOnNode := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{domain.JobId: "label"},
		},
		Spec: v1.PodSpec{
			NodeName: "Node2",
		},
	}
	node := v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Node1",
		},
	}

	pods := []*v1.Pod{&podOnNode}
	nodes := []*v1.Node{&node}

	result := getAllPodsRequiringResourceOnProcessingNodes(pods, nodes)

	assert.Equal(t, len(result), 0)
}

func TestGetUsageByQueue_HasAnEntryPerQueue(t *testing.T) {
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", podResource)
	queue1Pod2 := makePodWithResource("queue1", podResource)
	queue2Pod1 := makePodWithResource("queue2", podResource)

	pods := []*v1.Pod{&queue1Pod1, &queue1Pod2, &queue2Pod1}

	result := GetAllocationByQueue(pods)
	assert.Equal(t, len(result), 2)
	assert.True(t, hasKey(result, "queue1"))
	assert.True(t, hasKey(result, "queue2"))
}

func TestGetUsageByQueue_SkipsPodsWithoutQueue(t *testing.T) {
	podResource := makeResourceList(2, 50)
	pod := makePodWithResource("", podResource)
	pod.Labels = make(map[string]string)

	result := GetAllocationByQueue([]*v1.Pod{&pod})
	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func TestGetUsageByQueue_AggregatesPodResourcesInAQueue(t *testing.T) {
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", podResource)
	queue1Pod2 := makePodWithResource("queue1", podResource)

	pods := []*v1.Pod{&queue1Pod1, &queue1Pod2}

	expectedResource := makeResourceList(4, 100)
	expectedResult := map[string]common.ComputeResources{"queue1": common.FromResourceList(expectedResource)}

	result := GetAllocationByQueue(pods)
	assert.Equal(t, result, expectedResult)
}

func TestGetUsageByQueue_HandlesEmptyList(t *testing.T) {
	var pods []*v1.Pod

	result := GetAllocationByQueue(pods)

	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func TestGetAllocatedResourceByNodeName(t *testing.T) {
	podResource := makeResourceList(2, 50)
	pod1 := makePodWithResource("queue1", podResource)
	pod2 := makePodWithResource("queue1", podResource)
	pod3 := makePodWithResource("queue1", podResource)
	pod1.Spec.NodeName = "node1"
	pod2.Spec.NodeName = "node2"
	pod3.Spec.NodeName = "node2"
	pods := []*v1.Pod{&pod1, &pod2, &pod3}

	allocatedResource := getAllocatedResourceByNodeName(pods)
	assert.Equal(t, map[string]common.ComputeResources{
		"node1": common.FromResourceList(makeResourceList(2, 50)),
		"node2": common.FromResourceList(makeResourceList(4, 100)),
	}, allocatedResource)
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

func makePodWithResource(queue string, resource v1.ResourceList) v1.Pod {
	pod := v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: resource,
						Limits:   resource,
					},
				},
			},
		},
	}
	if queue != "" {
		pod.ObjectMeta = metav1.ObjectMeta{
			Labels: map[string]string{domain.JobId: util2.NewULID(), domain.Queue: queue},
		}
	}
	return pod
}
