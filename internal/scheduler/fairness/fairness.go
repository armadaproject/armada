package fairness

import (
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// QueueRepository is a minimal representation of a queue repository used for computing fairness.
type QueueRepository interface {
	GetQueue(name string) (Queue, bool)
}

// Queue is a minimal representation of a queue used for computing fairness.
type Queue interface {
	// GetAllocation returns the current allocation of the queue.
	GetAllocation() schedulerobjects.ResourceList
	GetWeight() float64
}

// FairnessCostProvider captures algorithms to compute the cost of an allocation.
type FairnessCostProvider interface {
	UnweightedCostFromQueue(queue Queue) float64
	UnweightedCostFromAllocation(allocation schedulerobjects.ResourceList) float64
	WeightedCostFromQueue(queue Queue) float64
	WeightedCostFromAllocation(allocation schedulerobjects.ResourceList, weight float64) float64
}

type resourceToConsider struct {
	Name       string
	Multiplier float64
}

type DominantResourceFairness struct {
	// Total resources across all nodes.
	totalResources schedulerobjects.ResourceList
	// Resources considered when computing DominantResourceFairness.
	resourcesToConsider []resourceToConsider
}

func NewDominantResourceFairness(totalResources schedulerobjects.ResourceList, config configuration.SchedulingConfig) (*DominantResourceFairness, error) {
	if len(config.DominantResourceFairnessResourcesToConsider) != 0 && len(config.ExperimentalDominantResourceFairnessResourcesToConsider) != 0 {
		return nil, errors.New("config error - only one of DominantResourceFairnessResourcesToConsider and ExperimentalDominantResourceFairnessResourcesToConsider should be set")
	}
	for _, rtc := range config.ExperimentalDominantResourceFairnessResourcesToConsider {
		if rtc.Multiplier < 0 {
			return nil, fmt.Errorf("config error - ExperimentalDominantResourceFairnessResourcesToConsider has negative multiplier for resource %s", rtc.Name)
		}
	}

	var resourcesToConsider []resourceToConsider
	if len(config.DominantResourceFairnessResourcesToConsider) > 0 {
		resourcesToConsider = slices.Map(config.DominantResourceFairnessResourcesToConsider, func(n string) resourceToConsider {
			return resourceToConsider{Name: n, Multiplier: 1}
		})
	} else if len(config.ExperimentalDominantResourceFairnessResourcesToConsider) > 0 {
		resourcesToConsider = slices.Map(config.ExperimentalDominantResourceFairnessResourcesToConsider, func(r configuration.DominantResourceFairnessResource) resourceToConsider {
			return resourceToConsider{Name: r.Name, Multiplier: defaultMultiplier(r.Multiplier)}
		})
	} else {
		return nil, errors.New("config error - DominantResourceFairnessResourcesToConsider and ExperimentalDominantResourceFairnessResourcesToConsider are both empty")
	}

	return &DominantResourceFairness{
		totalResources:      totalResources,
		resourcesToConsider: resourcesToConsider,
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

func (f *DominantResourceFairness) WeightedCostFromAllocation(allocation schedulerobjects.ResourceList, weight float64) float64 {
	return f.UnweightedCostFromAllocation(allocation) / weight
}

func (f *DominantResourceFairness) UnweightedCostFromAllocation(allocation schedulerobjects.ResourceList) float64 {
	var cost float64
	for _, t := range f.resourcesToConsider {
		capacity := f.totalResources.Get(t.Name)
		if capacity.Equal(resource.Quantity{}) {
			// Ignore any resources with zero capacity.
			continue
		}
		q := allocation.Get(t.Name)
		tcost := q.AsApproximateFloat64() / capacity.AsApproximateFloat64() * t.Multiplier
		if tcost > cost {
			cost = tcost
		}
	}
	return cost
}
