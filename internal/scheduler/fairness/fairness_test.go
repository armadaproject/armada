package fairness

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type MinimalQueue struct {
	allocation schedulerobjects.ResourceList
	weight     float64
}

func (q MinimalQueue) GetAllocation() schedulerobjects.ResourceList {
	return q.allocation
}

func (q MinimalQueue) GetWeight() float64 {
	return q.weight
}

func TestNewDominantResourceFairness(t *testing.T) {
	_, err := NewDominantResourceFairness(
		schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
			},
		},
		[]string{},
	)
	require.Error(t, err)
}

func TestDominantResourceFairness(t *testing.T) {
	tests := map[string]struct {
		totalResources      schedulerobjects.ResourceList
		resourcesToConsider []string
		allocation          schedulerobjects.ResourceList
		weight              float64
		expectedCost        float64
	}{
		"single resource 1": {
			totalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
					"baz": resource.MustParse("3"),
				},
			},
			resourcesToConsider: []string{"foo", "bar"},
			allocation: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0.5"),
				},
			},
			weight:       1.0,
			expectedCost: 0.5,
		},
		"single resource 2": {
			totalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
					"baz": resource.MustParse("3"),
				},
			},
			resourcesToConsider: []string{"foo", "bar"},
			allocation: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"bar": resource.MustParse("0.5"),
				},
			},
			weight:       1.0,
			expectedCost: 0.25,
		},
		"multiple resources": {
			totalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
					"baz": resource.MustParse("3"),
				},
			},
			resourcesToConsider: []string{"foo", "bar"},
			allocation: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0.5"),
					"bar": resource.MustParse("1.1"),
				},
			},
			weight:       1.0,
			expectedCost: 1.1 / 2,
		},
		"considered resources": {
			totalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
					"baz": resource.MustParse("3"),
				},
			},
			resourcesToConsider: []string{"foo", "bar"},
			allocation: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0.5"),
					"baz": resource.MustParse("3"),
				},
			},
			weight:       1.0,
			expectedCost: 0.5,
		},
		"zero available resource": {
			totalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("0"),
					"baz": resource.MustParse("3"),
				},
			},
			resourcesToConsider: []string{"foo", "bar"},
			allocation: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0.5"),
					"bar": resource.MustParse("2.0"),
				},
			},
			weight:       1.0,
			expectedCost: 0.5,
		},
		"weight": {
			totalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
					"baz": resource.MustParse("3"),
				},
			},
			resourcesToConsider: []string{"foo", "bar"},
			allocation: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0.5"),
				},
			},
			weight:       2.0,
			expectedCost: 0.25,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f, err := NewDominantResourceFairness(tc.totalResources, tc.resourcesToConsider)
			require.NoError(t, err)
			assert.Equal(
				t,
				tc.expectedCost,
				f.CostFromAllocationAndWeight(tc.allocation, tc.weight),
			)
			assert.Equal(
				t,
				f.CostFromAllocationAndWeight(tc.allocation, tc.weight),
				f.CostFromQueue(MinimalQueue{allocation: tc.allocation, weight: tc.weight}),
			)
		})
	}
}
