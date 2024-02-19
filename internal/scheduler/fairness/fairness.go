package fairness

import (
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

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
	CostFromQueue(queue Queue) float64
	CostFromAllocationAndWeight(allocation schedulerobjects.ResourceList, weight float64) float64
}

type DominantResourceFairness struct {
	// Total resources across all nodes.
	totalResources schedulerobjects.ResourceList
	// Resources considered when computing DominantResourceFairness.
	resourcesToConsider []string
}

func NewDominantResourceFairness(totalResources schedulerobjects.ResourceList, resourcesToConsider []string) (*DominantResourceFairness, error) {
	if len(resourcesToConsider) == 0 {
		return nil, errors.New("resourcesToConsider is empty")
	}
	return &DominantResourceFairness{
		totalResources:      totalResources,
		resourcesToConsider: resourcesToConsider,
	}, nil
}

func (f *DominantResourceFairness) CostFromQueue(queue Queue) float64 {
	return f.CostFromAllocationAndWeight(queue.GetAllocation(), queue.GetWeight())
}

func (f *DominantResourceFairness) CostFromAllocationAndWeight(allocation schedulerobjects.ResourceList, weight float64) float64 {
	var cost float64
	for _, t := range f.resourcesToConsider {
		capacity := f.totalResources.Get(t)
		if capacity.Equal(resource.Quantity{}) {
			// Ignore any resources with zero capacity.
			continue
		}
		q := allocation.Get(t)
		tcost := float64(q.MilliValue()) / float64(capacity.MilliValue())
		if tcost > cost {
			cost = tcost
		}
	}
	return cost / weight
}
