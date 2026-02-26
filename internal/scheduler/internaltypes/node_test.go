package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/pointer"
	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
)

func TestNode(t *testing.T) {
	resourceListFactory, err := NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "memory", Resolution: resource.MustParse("1")},
			{Name: "cpu", Resolution: resource.MustParse("1m")},
		},
		nil,
	)
	assert.Nil(t, err)

	const id = "id"
	const reportingNodeType = "type"
	const pool = "pool"
	const index = uint64(1)
	const executor = "executor"
	const name = "name"
	taints := []v1.Taint{
		{
			Key:   "foo",
			Value: "bar",
		},
	}
	labels := map[string]string{
		"key": "value",
	}
	totalResources := resourceListFactory.FromNodeProto(
		map[string]*resource.Quantity{
			"cpu":    pointer.MustParseResource("16"),
			"memory": pointer.MustParseResource("32Gi"),
		},
	)
	allocatableResources := resourceListFactory.FromNodeProto(
		map[string]*resource.Quantity{
			"cpu":    pointer.MustParseResource("8"),
			"memory": pointer.MustParseResource("16Gi"),
		},
	)
	allocatableByPriority := map[int32]ResourceList{
		1: resourceListFactory.FromNodeProto(
			map[string]*resource.Quantity{
				"cpu":    pointer.MustParseResource("0"),
				"memory": pointer.MustParseResource("0Gi"),
			},
		),
		2: resourceListFactory.FromNodeProto(
			map[string]*resource.Quantity{
				"cpu":    pointer.MustParseResource("8"),
				"memory": pointer.MustParseResource("16Gi"),
			},
		),
		3: resourceListFactory.FromNodeProto(
			map[string]*resource.Quantity{
				"cpu":    pointer.MustParseResource("16"),
				"memory": pointer.MustParseResource("32Gi"),
			},
		),
	}
	allocatedByQueue := map[string]ResourceList{
		"queue": resourceListFactory.FromJobResourceListIgnoreUnknown(
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("8"),
				"memory": resource.MustParse("16Gi"),
			},
		),
	}
	allocatedByJobId := map[string]ResourceList{
		"jobId": resourceListFactory.FromJobResourceListIgnoreUnknown(
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("8"),
				"memory": resource.MustParse("16Gi"),
			},
		),
	}
	evictedJobRunIds := map[string]bool{
		"jobId":        false,
		"evictedJobId": true,
	}
	keys := [][]byte{
		{
			0, 1, 255,
		},
	}

	nodeType := NewNodeType(
		taints,
		labels,
		map[string]bool{"foo": true},
		map[string]bool{"key": true},
	)

	node := CreateNode(
		id,
		nodeType,
		index,
		executor,
		name,
		pool,
		reportingNodeType,
		taints,
		labels,
		false,
		totalResources,
		allocatableResources,
		allocatableByPriority,
		allocatedByQueue,
		allocatedByJobId,
		evictedJobRunIds,
		keys,
	)

	assert.Equal(t, id, node.GetId())
	assert.Equal(t, reportingNodeType, node.GetReportingNodeType())
	assert.Equal(t, nodeType.GetId(), node.GetNodeTypeId())
	assert.Equal(t, nodeType.GetId(), node.GetNodeType().GetId())
	assert.Equal(t, index, node.GetIndex())
	assert.Equal(t, executor, node.GetExecutor())
	assert.Equal(t, name, node.GetName())
	assert.Equal(t, taints, node.GetTaints())
	assert.Equal(t, labels, node.GetLabels())
	assert.Equal(t, totalResources, node.GetTotalResources())
	assert.Equal(t, allocatableByPriority, node.AllocatableByPriority)
	assert.Equal(t, allocatedByQueue, node.AllocatedByQueue)
	assert.Equal(t, allocatedByJobId, node.AllocatedByJobId)
	assert.Equal(t, keys, node.Keys)

	val, ok := node.GetLabelValue("key")
	assert.True(t, ok)
	assert.Equal(t, "value", val)

	val, ok = node.GetLabelValue("missing")
	assert.False(t, ok)
	assert.Empty(t, val)

	tolerations := node.GetTolerationsForTaints()
	assert.Equal(t, []v1.Toleration{{Key: "foo", Value: "bar"}}, tolerations)

	nodeCopy := node.DeepCopyNilKeys()
	node.Keys = nil // UnsafeCopy() sets Keys to nil
	assert.Equal(t, node, nodeCopy)
}

func TestMarkResourceUnallocatable(t *testing.T) {
	resourceListFactory, err := NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "cpu", Resolution: resource.MustParse("1m")},
		},
		nil,
	)
	require.Nil(t, err)

	allocatableResources := makeCpuResourceList(resourceListFactory, "10")
	allocatableByPriority := map[int32]ResourceList{
		1: makeCpuResourceList(resourceListFactory, "8"),
		2: makeCpuResourceList(resourceListFactory, "6"),
	}

	node := createNode(allocatableResources, allocatableByPriority)

	unallocatable := makeCpuResourceList(resourceListFactory, "2")
	expectedAllocatableResources := makeCpuResourceList(resourceListFactory, "8")
	expectedAllocatableByPriority := map[int32]ResourceList{
		1: makeCpuResourceList(resourceListFactory, "6"),
		2: makeCpuResourceList(resourceListFactory, "4"),
	}

	result := node.MarkResourceUnallocatable(unallocatable)

	assert.Equal(t, expectedAllocatableResources, result.allocatableResources)
	assert.Equal(t, expectedAllocatableByPriority, result.AllocatableByPriority)
}

func TestMarkResourceUnallocatable_ProtectsFromNegativeValues(t *testing.T) {
	resourceListFactory, err := NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "cpu", Resolution: resource.MustParse("1m")},
		},
		nil,
	)
	assert.Nil(t, err)

	allocatableResources := makeCpuResourceList(resourceListFactory, "10")
	allocatableByPriority := map[int32]ResourceList{
		1: makeCpuResourceList(resourceListFactory, "8"),
		2: makeCpuResourceList(resourceListFactory, "6"),
	}

	node := createNode(allocatableResources, allocatableByPriority)

	unallocatable := makeCpuResourceList(resourceListFactory, "9")
	expectedAllocatableResources := makeCpuResourceList(resourceListFactory, "1")
	expectedAllocatableByPriority := map[int32]ResourceList{
		1: makeCpuResourceList(resourceListFactory, "0"),
		2: makeCpuResourceList(resourceListFactory, "0"),
	}

	result := node.MarkResourceUnallocatable(unallocatable)

	assert.Equal(t, expectedAllocatableResources, result.allocatableResources)
	assert.Equal(t, expectedAllocatableByPriority, result.AllocatableByPriority)
}

func makeCpuResourceList(factory *ResourceListFactory, cpu string) ResourceList {
	return factory.FromNodeProto(
		map[string]*resource.Quantity{
			"cpu": pointer.MustParseResource(cpu),
		},
	)
}

func createNode(allocatableResource ResourceList, allocatableByPriority map[int32]ResourceList) *Node {
	const id = "id"
	const reportingNodeType = "re"
	const pool = "pool"
	const index = uint64(1)
	const executor = "executor"
	const name = "name"
	node := CreateNode(
		id,
		nil,
		index,
		executor,
		name,
		pool,
		reportingNodeType,
		nil,
		nil,
		false,
		allocatableResource,
		allocatableResource,
		allocatableByPriority,
		nil,
		nil,
		nil,
		nil,
	)

	return node
}
