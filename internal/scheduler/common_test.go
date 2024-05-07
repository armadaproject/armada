package scheduler

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

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
