package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestNodeUnsafeCopy(t *testing.T) {
	node := &Node{
		Id:       "id",
		Index:    1,
		Executor: "executor",
		Name:     "name",
		Taints: []v1.Taint{
			{
				Key:   "foo",
				Value: "bar",
			},
		},
		Labels: map[string]string{
			"key": "value",
		},
		TotalResources: schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("16"),
				"memory": resource.MustParse("32Gi"),
			},
		},
		Keys: [][]byte{
			{
				0, 1, 255,
			},
		},
		NodeTypeId: 123,
		AllocatableByPriority: schedulerobjects.AllocatableByPriorityAndResourceType{
			1: {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("0"),
					"memory": resource.MustParse("0Gi"),
				},
			},
			2: {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("8"),
					"memory": resource.MustParse("16Gi"),
				},
			},
			3: {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("16"),
					"memory": resource.MustParse("32Gi"),
				},
			},
		},
		AllocatedByQueue: map[string]schedulerobjects.ResourceList{
			"queue": {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("8"),
					"memory": resource.MustParse("16Gi"),
				},
			},
		},
		AllocatedByJobId: map[string]schedulerobjects.ResourceList{
			"jobId": {
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("8"),
					"memory": resource.MustParse("16Gi"),
				},
			},
		},
		EvictedJobRunIds: map[string]bool{
			"jobId":        false,
			"evictedJobId": true,
		},
	}
	nodeCopy := node.UnsafeCopy()
	// TODO(albin): Add more tests here.
	assert.Equal(t, node.Id, nodeCopy.Id)
}
