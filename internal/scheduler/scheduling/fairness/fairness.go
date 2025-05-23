package fairness

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// QueueRepository is a minimal representation of a queue repository used for computing fairness.
type QueueRepository interface {
	GetQueue(name string) (Queue, bool)
}

// Queue is a minimal representation of a queue used for computing fairness.
type Queue interface {
	// GetAllocation returns the current allocation of the queue.
	GetAllocation() internaltypes.ResourceList
	GetWeight() float64
}

// FairnessCostProvider captures algorithms to compute the cost of an allocation.
type FairnessCostProvider interface {
	UnweightedCostFromQueue(queue Queue) float64
	UnweightedCostFromAllocation(allocation internaltypes.ResourceList) float64
	WeightedCostFromQueue(queue Queue) float64
	WeightedCostFromAllocation(allocation internaltypes.ResourceList, weight float64) float64
}

type DominantResourceFairness struct {
	// Total resources across all nodes.
	totalResources internaltypes.ResourceList
	// Weight (defined in config) for each resource.
	// Typically 1.0 (we care about that resource when assigning costs),
	// or 0.0 (we don't care). However other values are possible.
	multipliers internaltypes.ResourceFractionList
}

func NewDominantResourceFairness(totalResources internaltypes.ResourceList, pool string, config configuration.SchedulingConfig) (*DominantResourceFairness, error) {
	if totalResources.IsEmpty() {
		return &DominantResourceFairness{}, nil
	}

	resourcesToConsider := config.DominantResourceFairnessResourcesToConsider
	experimentalResourcesToConsider := config.ExperimentalDominantResourceFairnessResourcesToConsider

	poolConfig := config.GetPoolConfig(pool)
	if poolConfig != nil {
		if len(poolConfig.DominantResourceFairnessResourcesToConsider) > 0 {
			resourcesToConsider = poolConfig.DominantResourceFairnessResourcesToConsider
		}
		if len(poolConfig.ExperimentalDominantResourceFairnessResourcesToConsider) > 0 {
			experimentalResourcesToConsider = poolConfig.ExperimentalDominantResourceFairnessResourcesToConsider
		}
	}

	if len(resourcesToConsider) != 0 && len(experimentalResourcesToConsider) != 0 {
		return nil, errors.New(
			fmt.Sprintf("config error for pool %s - only one of DominantResourceFairnessResourcesToConsider and ExperimentalDominantResourceFairnessResourcesToConsider should be set", pool),
		)
	}
	for _, rtc := range experimentalResourcesToConsider {
		if rtc.Multiplier < 0 {
			return nil, fmt.Errorf("config error - ExperimentalDominantResourceFairnessResourcesToConsider has negative multiplier for resource %s", rtc.Name)
		}
	}

	var multipliers map[string]float64
	if len(resourcesToConsider) > 0 {
		multipliers = maps.FromSlice(resourcesToConsider, func(n string) string {
			return n
		}, func(n string) float64 {
			return 1.0
		})
	} else if len(experimentalResourcesToConsider) > 0 {
		multipliers = maps.FromSlice(experimentalResourcesToConsider, func(r configuration.DominantResourceFairnessResource) string {
			return r.Name
		}, func(r configuration.DominantResourceFairnessResource) float64 {
			return defaultMultiplier(r.Multiplier)
		})
	} else {
		return nil, errors.New("config error - DominantResourceFairnessResourcesToConsider and ExperimentalDominantResourceFairnessResourcesToConsider are both empty")
	}

	return &DominantResourceFairness{
		totalResources: totalResources,
		multipliers:    totalResources.Factory().MakeResourceFractionList(multipliers, 0.0),
	}, nil
}

func defaultMultiplier(multiplier float64) float64 {
	if !(multiplier > 0) {
		return 1
	}
	return multiplier
}

func (f *DominantResourceFairness) WeightedCostFromQueue(queue Queue) float64 {
	return f.UnweightedCostFromQueue(queue) / queue.GetWeight()
}

func (f *DominantResourceFairness) UnweightedCostFromQueue(queue Queue) float64 {
	return f.UnweightedCostFromAllocation(queue.GetAllocation())
}

func (f *DominantResourceFairness) WeightedCostFromAllocation(allocation internaltypes.ResourceList, weight float64) float64 {
	return f.UnweightedCostFromAllocation(allocation) / weight
}

func (f *DominantResourceFairness) UnweightedCostFromAllocation(allocation internaltypes.ResourceList) float64 {
	return max(0, allocation.DivideZeroOnError(f.totalResources).Multiply(f.multipliers).Max())
}
