package types

import "golang.org/x/exp/maps"

type PriorityClass struct {
	Priority int32
	// If true, Armada may preempt jobs of this class to improve fairness.
	Preemptible bool
	// Limits resources assigned to jobs of this priority class.
	// Specifically, jobs of this priority class are only scheduled if doing so does not exceed this limit.
	MaximumResourceFractionPerQueue map[string]float64
	// Per-pool override of MaximumResourceFractionPerQueue.
	// If missing for a particular pool, MaximumResourceFractionPerQueue is used instead for that pool.
	MaximumResourceFractionPerQueueByPool map[string]map[string]float64
}

func (priorityClass PriorityClass) Equal(other PriorityClass) bool {
	if priorityClass.Priority != other.Priority {
		return false
	}
	if priorityClass.Preemptible != other.Preemptible {
		return false
	}
	if !maps.Equal(priorityClass.MaximumResourceFractionPerQueue, other.MaximumResourceFractionPerQueue) {
		return false
	}
	if len(priorityClass.MaximumResourceFractionPerQueueByPool) != len(other.MaximumResourceFractionPerQueueByPool) {
		return false
	}
	for k, v := range priorityClass.MaximumResourceFractionPerQueueByPool {
		if !maps.Equal(v, other.MaximumResourceFractionPerQueueByPool[k]) {
			return false
		}
	}
	return true
}
