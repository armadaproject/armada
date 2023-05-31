package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

func TestPodRequirementFromLegacySchedulerJob(t *testing.T) {
	resourceLimit := v1.ResourceList{
		"cpu":               resource.MustParse("1"),
		"memory":            resource.MustParse("128Mi"),
		"ephemeral-storage": resource.MustParse("8Gi"),
	}
	requirements := v1.ResourceRequirements{
		Limits:   resourceLimit,
		Requests: resourceLimit,
	}

	j := &api.Job{
		Id:       util.NewULID(),
		Queue:    "test",
		JobSetId: "set1",
		Priority: 1,
		Annotations: map[string]string{
			"something":                             "test",
			configuration.GangIdAnnotation:          "gang-id",
			configuration.GangCardinalityAnnotation: "1",
		},
		PodSpecs: []*v1.PodSpec{
			{
				Containers: []v1.Container{
					{
						Resources: requirements,
					},
				},
				PriorityClassName: "armada-default",
			},
		},
	}
	expected := &schedulerobjects.PodRequirements{
		Priority:             1,
		PreemptionPolicy:     string(v1.PreemptLowerPriority),
		ResourceRequirements: requirements,
		Annotations: map[string]string{
			configuration.GangIdAnnotation:          "gang-id",
			configuration.GangCardinalityAnnotation: "1",
			schedulerconfig.JobIdAnnotation:         j.Id,
			schedulerconfig.QueueAnnotation:         j.Queue,
		},
	}
	actual := PodRequirementFromLegacySchedulerJob(j, map[string]configuration.PriorityClass{"armada-default": {Priority: int32(1)}})
	assert.Equal(t, expected, actual)
}

func BenchmarkResourceListAsWeightedMillis(b *testing.B) {
	rl := schedulerobjects.NewResourceList(3)
	rl.Set("cpu", resource.MustParse("2"))
	rl.Set("memory", resource.MustParse("10Gi"))
	rl.Set("nvidia.com/gpu", resource.MustParse("1"))
	weights := map[string]float64{
		"cpu":            1,
		"memory":         0.1,
		"nvidia.com/gpu": 10,
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ResourceListAsWeightedMillis(weights, rl)
	}
}
