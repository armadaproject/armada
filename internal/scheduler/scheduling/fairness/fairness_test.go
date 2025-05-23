package fairness

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

const poolName = "pool"

type MinimalQueue struct {
	allocation internaltypes.ResourceList
	weight     float64
}

func (q MinimalQueue) GetAllocation() internaltypes.ResourceList {
	return q.allocation
}

func (q MinimalQueue) GetWeight() float64 {
	return q.weight
}

func TestNewDominantResourceFairness(t *testing.T) {
	rlFactory := makeTestResourceListFactory()
	_, err := NewDominantResourceFairness(
		fooBarBaz(rlFactory, "1", "0", "0"),
		poolName,
		configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{}},
	)
	require.Error(t, err)
}

func TestNewDominantResourceFairness_InvalidConfig(t *testing.T) {
	tests := map[string]struct {
		config configuration.SchedulingConfig
	}{
		"must not provide both types of config": {
			config: configuration.SchedulingConfig{
				DominantResourceFairnessResourcesToConsider:             []string{"foo", "bar"},
				ExperimentalDominantResourceFairnessResourcesToConsider: []configuration.DominantResourceFairnessResource{{"foo", 1}, {"bar", 1}},
			},
		},
		"pool overrive - invalid combination": {
			config: configuration.SchedulingConfig{
				DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"},
				Pools: []configuration.PoolConfig{
					{
						Name: poolName,
						ExperimentalDominantResourceFairnessResourcesToConsider: []configuration.DominantResourceFairnessResource{{"foo", 1}, {"bar", 0.5}},
					},
				},
			},
		},
		"pool overrive - invalid combination 2": {
			config: configuration.SchedulingConfig{
				ExperimentalDominantResourceFairnessResourcesToConsider: []configuration.DominantResourceFairnessResource{{"foo", 1}, {"bar", 1}},
				Pools: []configuration.PoolConfig{
					{
						Name: poolName,
						DominantResourceFairnessResourcesToConsider: []string{"foo"},
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rlFactory := makeTestResourceListFactory()
			_, err := NewDominantResourceFairness(
				fooBarBaz(rlFactory, "1", "0", "0"),
				poolName,
				tc.config,
			)
			assert.Error(t, err)
		})
	}
}

func TestDominantResourceFairness(t *testing.T) {
	rlFactory := makeTestResourceListFactory()

	tests := map[string]struct {
		totalResources internaltypes.ResourceList
		config         configuration.SchedulingConfig
		allocation     internaltypes.ResourceList
		weight         float64
		expectedCost   float64
	}{
		"single resource 1": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"}},
			allocation:     fooBarBaz(rlFactory, "0.5", "0", "0"),
			weight:         1.0,
			expectedCost:   0.5,
		},
		"single resource 2": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"}},
			allocation:     fooBarBaz(rlFactory, "0", "0.5", "0"),
			weight:         1.0,
			expectedCost:   0.25,
		},

		"pool override - experimental resource": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"}},
			allocation:     fooBarBaz(rlFactory, "0", "0.5", "0"),
			weight:         1.0,
			expectedCost:   0.25,
		},
		"multiple resources": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"}},
			allocation:     fooBarBaz(rlFactory, "0.5", "1.1", "0"),
			weight:         1.0,
			expectedCost:   1.1 / 2,
		},
		"considered resources": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"}},
			allocation:     fooBarBaz(rlFactory, "0.5", "0", "3"),
			weight:         1.0,
			expectedCost:   0.5,
		},
		"zero available resource": {
			totalResources: fooBarBaz(rlFactory, "1", "0", "3"),
			config:         configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"}},
			allocation:     fooBarBaz(rlFactory, "0.5", "2.0", "0"),
			weight:         1.0,
			expectedCost:   0.5,
		},
		"weight": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"}},
			allocation:     fooBarBaz(rlFactory, "0.5", "0", "0"),
			weight:         2.0,
			expectedCost:   0.25,
		},
		"experimental config": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{ExperimentalDominantResourceFairnessResourcesToConsider: []configuration.DominantResourceFairnessResource{{"foo", 1}, {"bar", 1}}},
			allocation:     fooBarBaz(rlFactory, "0.5", "1.1", "0"),
			weight:         1.0,
			expectedCost:   1.1 / 2,
		},
		"experimental config defaults multipliers to one": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{ExperimentalDominantResourceFairnessResourcesToConsider: []configuration.DominantResourceFairnessResource{{"foo", 0}, {"bar", 0}}},
			allocation:     fooBarBaz(rlFactory, "0.5", "1.1", "0"),
			weight:         1.0,
			expectedCost:   1.1 / 2,
		},
		"experimental config non-unit multiplier": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config:         configuration.SchedulingConfig{ExperimentalDominantResourceFairnessResourcesToConsider: []configuration.DominantResourceFairnessResource{{"foo", 4}, {"bar", 1}}},
			allocation:     fooBarBaz(rlFactory, "0.5", "1.1", "0"),
			weight:         1.0,
			expectedCost:   2,
		},
		"pool override - dominant config": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config: configuration.SchedulingConfig{
				DominantResourceFairnessResourcesToConsider: []string{"foo", "bar"},
				Pools: []configuration.PoolConfig{
					{
						Name: poolName,
						DominantResourceFairnessResourcesToConsider: []string{"foo"},
					},
				},
			},
			allocation:   fooBarBaz(rlFactory, "0.5", "2", "0"),
			weight:       1.0,
			expectedCost: 0.5,
		},
		"pool override - experimental config": {
			totalResources: fooBarBaz(rlFactory, "1", "2", "3"),
			config: configuration.SchedulingConfig{
				ExperimentalDominantResourceFairnessResourcesToConsider: []configuration.DominantResourceFairnessResource{{"foo", 1}, {"bar", 1}},
				Pools: []configuration.PoolConfig{
					{
						Name: poolName,
						ExperimentalDominantResourceFairnessResourcesToConsider: []configuration.DominantResourceFairnessResource{{"foo", 1}, {"bar", 0.5}},
					},
				},
			},
			allocation:   fooBarBaz(rlFactory, "0.5", "2", "0"),
			weight:       1.0,
			expectedCost: 0.5,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f, err := NewDominantResourceFairness(tc.totalResources, poolName, tc.config)
			require.NoError(t, err)
			assert.Equal(
				t,
				tc.expectedCost,
				f.WeightedCostFromAllocation(tc.allocation, tc.weight),
			)
			assert.Equal(
				t,
				f.WeightedCostFromAllocation(tc.allocation, tc.weight),
				f.WeightedCostFromQueue(MinimalQueue{allocation: tc.allocation, weight: tc.weight}),
			)
		})
	}
}

func makeTestResourceListFactory() *internaltypes.ResourceListFactory {
	rlFactory, err := internaltypes.NewResourceListFactory(
		[]configuration.ResourceType{
			{Name: "foo"},
			{Name: "bar"},
		},
		[]configuration.FloatingResourceConfig{
			{Name: "baz"},
		},
	)
	if err != nil {
		panic(err)
	}
	return rlFactory
}

func fooBarBaz(factory *internaltypes.ResourceListFactory, foo, bar, baz string) internaltypes.ResourceList {
	return factory.FromNodeProto(map[string]*resource.Quantity{
		"foo": pointer.MustParseResource(foo),
		"bar": pointer.MustParseResource(bar),
		"baz": pointer.MustParseResource(baz),
	})
}
