package scheduling2

import (
	"cmp"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

// Comparator function (sort by element's priority value in descending order)
func byQueueCost(a, b Element) int {
	return -cmp.Compare(a.priority, b.priority) // "-" descending order
}

type Scheduler struct {
	nodeDb                model.NodeDb
	floatingResourceTypes *floatingresources.FloatingResourceTypes
	constraints           schedulerconstraints.SchedulingConstraints
}

func Schedule(jobIterators map[string]model.JobIterator) {

}
