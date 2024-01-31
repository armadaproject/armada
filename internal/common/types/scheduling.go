package types

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type AwayNodeType struct {
	// Priority is the priority class priority that the scheduler should use
	// when scheduling "away" jobs of this priority class on the the node type
	// referenced by WellKnownNodeTypeName; it overrides the Priority field of
	// PriorityClass.
	Priority int32 `validate:"gte=0"`
	// WellKnownNodeTypeName is the Name of the WellKnownNodeType in question.
	WellKnownNodeTypeName string `validate:"required"`
}

type PriorityClass struct {
	Priority int32 `validate:"gte=0"`
	// If true, Armada may preempt jobs of this class to improve fairness.
	Preemptible bool
	// Limits resources assigned to jobs of this priority class.
	// Specifically, jobs of this priority class are only scheduled if doing so does not exceed this limit.
	MaximumResourceFractionPerQueue map[string]float64
	// Per-pool override of MaximumResourceFractionPerQueue.
	// If missing for a particular pool, MaximumResourceFractionPerQueue is used instead for that pool.
	MaximumResourceFractionPerQueueByPool map[string]map[string]float64
	// AwayNodeTypes is the set of node types that jobs of this priority class
	// can be scheduled on as "away" jobs (i.e., with reduced priority).
	//
	// The scheduler first tries to schedule jobs of this priority class as
	// "home" jobs, and then tries the elements of this slice in order.
	AwayNodeTypes []AwayNodeType `validate:"dive"`
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

func AllowedPriorities(priorityClasses map[string]PriorityClass) []int32 {
	allowedPriorities := make([]int32, 0, len(priorityClasses))
	for _, priorityClass := range priorityClasses {
		allowedPriorities = append(allowedPriorities, priorityClass.Priority)
		for _, awayNodeType := range priorityClass.AwayNodeTypes {
			allowedPriorities = append(allowedPriorities, awayNodeType.Priority)
		}
	}
	slices.Sort(allowedPriorities)
	return slices.Compact(allowedPriorities)
}
