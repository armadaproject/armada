package scheduler

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

func TestGetPodRequirements(t *testing.T) {
	resourceLimit := v1.ResourceList{
		"cpu":               resource.MustParse("1"),
		"memory":            resource.MustParse("128Mi"),
		"ephemeral-storage": resource.MustParse("8Gi"),
	}

	// Hack to remove cached strings in quantities, which invalidate comparisons.
	q := resourceLimit["cpu"]
	q.Add(resource.Quantity{})
	resourceLimit["cpu"] = q
	q = resourceLimit["memory"]
	q.Add(resource.Quantity{})
	resourceLimit["memory"] = q
	q = resourceLimit["ephemeral-storage"]
	q.Add(resource.Quantity{})
	resourceLimit["ephemeral-storage"] = q

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
			"something":                             "test",
			configuration.GangIdAnnotation:          "gang-id",
			configuration.GangCardinalityAnnotation: "1",
		},
	}
	actual := j.GetPodRequirements(map[string]types.PriorityClass{"armada-default": {Priority: int32(1)}})
	assert.Equal(t, expected, actual)
}

func TestResourceListAsWeightedMillis(t *testing.T) {
	tests := map[string]struct {
		rl       schedulerobjects.ResourceList
		weights  map[string]float64
		expected int64
	}{
		"default": {
			rl: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("2"),
					"bar": resource.MustParse("10Gi"),
					"baz": resource.MustParse("1"),
				},
			},
			weights: map[string]float64{
				"foo": 1,
				"bar": 0.1,
				"baz": 10,
			},
			expected: (1 * 2 * 1000) + (1 * 1000 * 1024 * 1024 * 1024) + (10 * 1 * 1000),
		},
		"zeroes": {
			rl: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0"),
					"bar": resource.MustParse("1"),
					"baz": resource.MustParse("2"),
				},
			},
			weights: map[string]float64{
				"foo": 1,
				"bar": 0,
			},
			expected: 0,
		},
		"1Pi": {
			rl: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1Pi"),
				},
			},
			weights: map[string]float64{
				"foo": 1,
			},
			expected: int64(math.Pow(1024, 5)) * 1000,
		},
		"rounding": {
			rl: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
				},
			},
			weights: map[string]float64{
				"foo": 0.3006,
			},
			expected: 301,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.rl.AsWeightedMillis(tc.weights))
		})
	}
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
		rl.AsWeightedMillis(weights)
	}
}
