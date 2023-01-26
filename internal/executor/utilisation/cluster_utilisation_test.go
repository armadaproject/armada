package utilisation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	util2 "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/domain"
)

func TestCreateReportsOfQueueUsages(t *testing.T) {
	utilisationService := &ClusterUtilisationService{
		queueUtilisationService: NewPodUtilisationService(nil, nil, nil, nil),
	}

	var priority int32
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", podResource, &priority)
	queue1Pod2 := makePodWithResource("queue1", podResource, &priority)

	reports := utilisationService.createReportsOfQueueUsages([]*v1.Pod{&queue1Pod1, &queue1Pod2})

	expectedResource := map[string]resource.Quantity{
		"cpu":    *resource.NewQuantity(4, resource.DecimalSI),
		"memory": *resource.NewQuantity(100*1024*1024*1024, resource.DecimalSI),
	}

	assert.Equal(t, len(reports), 1)
	assert.Equal(t, reports[0].Name, "queue1")
	assert.Equal(t, reports[0].CountOfPodsByPhase, map[string]uint32{string(v1.PodRunning): 2})
	assert.Equal(t, reports[0].Resources, expectedResource)
	assert.Equal(t, reports[0].ResourcesUsed, map[string]resource.Quantity{})
}

func TestCreateReportsOfQueueUsages_WhenAllPending(t *testing.T) {
	utilisationService := &ClusterUtilisationService{
		queueUtilisationService: NewPodUtilisationService(nil, nil, nil, nil),
	}

	var priority int32
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", podResource, &priority)
	queue1Pod1.Status.Phase = v1.PodPending

	reports := utilisationService.createReportsOfQueueUsages([]*v1.Pod{&queue1Pod1})

	assert.Equal(t, len(reports), 1)
	assert.Equal(t, reports[0].Name, "queue1")
	assert.Equal(t, reports[0].CountOfPodsByPhase, map[string]uint32{string(v1.PodPending): 1})
	assert.Equal(t, reports[0].Resources, map[string]resource.Quantity{})
	assert.Equal(t, reports[0].ResourcesUsed, map[string]resource.Quantity{})
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
	var priority int32
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", podResource, &priority)
	queue1Pod2 := makePodWithResource("queue1", podResource, &priority)
	queue2Pod1 := makePodWithResource("queue2", podResource, &priority)

	pods := []*v1.Pod{&queue1Pod1, &queue1Pod2, &queue2Pod1}

	result := GetAllocationByQueue(pods)
	assert.Equal(t, len(result), 2)
	assert.True(t, hasKey(result, "queue1"))
	assert.True(t, hasKey(result, "queue2"))
}

func TestGetUsageByQueue_SkipsPodsWithoutQueue(t *testing.T) {
	var priority int32
	podResource := makeResourceList(2, 50)
	pod := makePodWithResource("", podResource, &priority)
	pod.Labels = make(map[string]string)

	result := GetAllocationByQueue([]*v1.Pod{&pod})
	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func TestGetUsageByQueue_AggregatesPodResourcesInAQueue(t *testing.T) {
	var priority int32
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", podResource, &priority)
	queue1Pod2 := makePodWithResource("queue1", podResource, &priority)

	pods := []*v1.Pod{&queue1Pod1, &queue1Pod2}

	expectedResource := makeResourceList(4, 100)
	expectedResult := map[string]armadaresource.ComputeResources{"queue1": armadaresource.FromResourceList(expectedResource)}

	result := GetAllocationByQueue(pods)
	assert.Equal(t, result, expectedResult)
}

func TestGetUsageByQueue_HandlesEmptyList(t *testing.T) {
	var pods []*v1.Pod

	result := GetAllocationByQueue(pods)

	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func TestGetAllocationByQueueAndPriority_HasAnEntryPerQueue(t *testing.T) {
	var priority int32
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", podResource, &priority)
	queue1Pod2 := makePodWithResource("queue1", podResource, &priority)
	queue2Pod1 := makePodWithResource("queue2", podResource, &priority)

	pods := []*v1.Pod{&queue1Pod1, &queue1Pod2, &queue2Pod1}

	result := GetAllocationByQueueAndPriority(pods)
	assert.Equal(t, len(result), 2)
	assert.True(t, hasKey(result, "queue1"))
	assert.True(t, hasKey(result, "queue2"))
}

func TestGetAllocationByQueueAndPriority_SkipsPodsWithoutQueue(t *testing.T) {
	var priority int32
	podResource := makeResourceList(2, 50)
	pod := makePodWithResource("", podResource, &priority)
	pod.Labels = make(map[string]string)

	result := GetAllocationByQueueAndPriority([]*v1.Pod{&pod})
	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func TestGetAllocationByQueueAndPriority_AggregatesPodResourcesInAQueue(t *testing.T) {
	var priority int32 = 10
	podResource := makeResourceList(2, 50)
	queue1Pod1 := makePodWithResource("queue1", podResource, &priority)
	queue1Pod2 := makePodWithResource("queue1", podResource, &priority)

	pods := []*v1.Pod{&queue1Pod1, &queue1Pod2}

	expectedResource := makeResourceList(4, 100)
	expectedResult := map[string]map[int32]armadaresource.ComputeResources{
		"queue1": {
			priority: armadaresource.FromResourceList(expectedResource),
		},
	}

	result := GetAllocationByQueueAndPriority(pods)
	assert.Equal(t, result, expectedResult)
}

func TestGetAllocationByQueueAndPriority_AggregatesResources(t *testing.T) {
	var priority1 int32 = 1
	var priority2 int32 = 2
	podResource := makeResourceList(2, 50)

	queue1Pod1 := makePodWithResource("queue1", podResource, &priority1)
	queue1Pod2 := makePodWithResource("queue1", podResource, &priority1)
	queue1Pod3 := makePodWithResource("queue1", podResource, &priority2)
	queue1Pod4 := makePodWithResource("queue1", podResource, &priority2)

	queue2Pod1 := makePodWithResource("queue2", podResource, &priority1)
	queue2Pod2 := makePodWithResource("queue2", podResource, &priority1)
	queue2Pod3 := makePodWithResource("queue2", podResource, &priority2)
	queue2Pod4 := makePodWithResource("queue2", podResource, &priority2)

	pods := []*v1.Pod{
		&queue1Pod1,
		&queue1Pod2,
		&queue1Pod3,
		&queue1Pod4,
		&queue2Pod1,
		&queue2Pod2,
		&queue2Pod3,
		&queue2Pod4,
	}

	expectedResource := makeResourceList(4, 100)
	expectedResult := map[string]map[int32]armadaresource.ComputeResources{
		"queue1": {
			priority1: armadaresource.FromResourceList(expectedResource),
			priority2: armadaresource.FromResourceList(expectedResource),
		},
		"queue2": {
			priority1: armadaresource.FromResourceList(expectedResource),
			priority2: armadaresource.FromResourceList(expectedResource),
		},
	}

	result := GetAllocationByQueueAndPriority(pods)
	assert.Equal(t, result, expectedResult)
}

func TestGetAllocationByQueueAndPriority_HandlesEmptyList(t *testing.T) {
	var pods []*v1.Pod

	result := GetAllocationByQueueAndPriority(pods)

	assert.NotNil(t, result)
	assert.Equal(t, len(result), 0)
}

func TestGetAllocatedResourceByNodeName(t *testing.T) {
	var priority int32
	podResource := makeResourceList(2, 50)
	pod1 := makePodWithResource("queue1", podResource, &priority)
	pod2 := makePodWithResource("queue1", podResource, &priority)
	pod3 := makePodWithResource("queue1", podResource, &priority)
	pod1.Spec.NodeName = "node1"
	pod2.Spec.NodeName = "node2"
	pod3.Spec.NodeName = "node2"
	pods := []*v1.Pod{&pod1, &pod2, &pod3}

	allocatedResource := getAllocatedResourceByNodeName(pods)
	assert.Equal(t, map[string]armadaresource.ComputeResources{
		"node1": armadaresource.FromResourceList(makeResourceList(2, 50)),
		"node2": armadaresource.FromResourceList(makeResourceList(4, 100)),
	}, allocatedResource)
}

func hasKey[K comparable, V any](m map[K]V, key K) bool {
	_, ok := m[key]
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

func makePodWithResource(queue string, resource v1.ResourceList, priority *int32) v1.Pod {
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
			Priority: priority,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: util2.NewULID(),
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}
	if queue != "" {
		pod.ObjectMeta.Labels = map[string]string{domain.JobId: util2.NewULID(), domain.Queue: queue}
	}
	return pod
}

func TestGetCordonedResource(t *testing.T) {
	nodes := []*v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node1",
			},
			Spec: v1.NodeSpec{
				Unschedulable: true,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node2",
			},
			Spec: v1.NodeSpec{
				Unschedulable: false,
			},
		},
	}
	pod1 := createTestPod("node1", "1", "10Gi")
	pod2 := createTestPod("node1", "2", "2Gi")
	pod3 := createTestPod("node2", "1", "10Gi")
	pods := []*v1.Pod{pod1, pod2, pod3}

	resources := getCordonedResource(nodes, pods)

	expected := armadaresource.ComputeResources{
		"cpu":    resource.MustParse("3"),
		"memory": resource.MustParse("12Gi"),
	}

	assert.True(t, expected.Equal(resources))
}

func createTestPod(nodeName string, cpu string, memory string) *v1.Pod {
	return &v1.Pod{
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"cpu":    resource.MustParse(cpu),
							"memory": resource.MustParse(memory),
						},
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse(cpu),
							"memory": resource.MustParse(memory),
						},
					},
				},
			},
		},
	}
}
