package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
)

func TestNode(t *testing.T) {
	resourceListFactory, err := MakeResourceListFactory([]schedulerconfiguration.ResourceType{
		{Name: "memory", Resolution: resource.MustParse("1")},
		{Name: "cpu", Resolution: resource.MustParse("1m")},
	})
	assert.Nil(t, err)

	const id = "id"
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
		map[string]resource.Quantity{
			"cpu":    resource.MustParse("16"),
			"memory": resource.MustParse("32Gi"),
		},
	)
	allocatableByPriority := map[int32]ResourceList{
		1: resourceListFactory.FromNodeProto(
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("0"),
				"memory": resource.MustParse("0Gi"),
			},
		),
		2: resourceListFactory.FromNodeProto(
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("8"),
				"memory": resource.MustParse("16Gi"),
			},
		),
		3: resourceListFactory.FromNodeProto(
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("16"),
				"memory": resource.MustParse("32Gi"),
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
		taints,
		labels,
		totalResources,
		allocatableByPriority,
		allocatedByQueue,
		allocatedByJobId,
		evictedJobRunIds,
		keys,
	)

	assert.Equal(t, id, node.GetId())
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
