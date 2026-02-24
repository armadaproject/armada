package types

import (
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

// AwayNodeTypeConditionOperator is the comparison operator for an AwayNodeTypeCondition.
type AwayNodeTypeConditionOperator string

const (
	AwayNodeTypeConditionOpGreaterThan AwayNodeTypeConditionOperator = ">"
	AwayNodeTypeConditionOpLessThan    AwayNodeTypeConditionOperator = "<"
	AwayNodeTypeConditionOpEqual       AwayNodeTypeConditionOperator = "=="
)

// AwayNodeTypeCondition is a threshold on a single resource.
// An AwayNodeType entry is only applied when ALL conditions are satisfied.
type AwayNodeTypeCondition struct {
	// Resource is the Kubernetes resource name, e.g. "cpu", "memory", "nvidia.com/gpu".
	Resource string `validate:"required"`
	// Operator is one of ">", ">=", "<", "<=", "==".
	Operator AwayNodeTypeConditionOperator `validate:"required"`
	// Value is a Kubernetes resource quantity string, e.g. "1", "4Gi".
	// Parsed and validated at config load time.
	Value armadaresource.ComputeResources `validate:"required"`
}

// WellKnownNodeTypeConfig is one entry within an AwayNodeType group.
// Its tolerations are included in the scheduling attempt only when ALL Conditions are met.
// An empty Conditions list means always apply (the common case).
type WellKnownNodeTypeConfig struct {
	// Name references a WellKnownNodeType defined in SchedulingConfig.WellKnownNodeTypes.
	Name       string                  `validate:"required"`
	Conditions []AwayNodeTypeCondition `validate:"dive"`
}

// EffectiveAwayNodeType is the pre-computed result for one away scheduling group.
// It is stored on the Job at creation time so that condition evaluation and
// taint-to-toleration resolution do not need to repeat on every scheduling cycle.
type EffectiveAwayNodeType struct {
	Priority    int32
	Tolerations []v1.Toleration
}

type AwayNodeType struct {
	// Priority is the priority class priority that the scheduler should use
	// when scheduling "away" jobs of this priority class on the node types in
	// this group; it overrides the Priority field of PriorityClass.
	Priority int32 `validate:"gte=0"`
	// WellKnownNodeTypes is the set of well-known node types in this scheduling group.
	// All entries whose Conditions match the job have their tolerations combined before
	// a single scheduling attempt, so the whole group costs exactly one scheduling round.
	// At least one entry is required.
	WellKnownNodeTypes []WellKnownNodeTypeConfig `validate:"required,min=1,dive"`
}

// PriorityClass represents an Armada-specific priority class used for scheduling and preemption.
// This is separate from Kubernetes PriorityClass resources - Armada manages preemption
// in its own scheduler before pods are ever submitted to Kubernetes.
type PriorityClass struct {
	// Priority controls scheduling order. Higher values get scheduled first and can
	// preempt running jobs with lower priority values (urgency-based preemption).
	Priority int32 `validate:"gte=0"`
	// Preemptible determines if jobs can be evicted to rebalance resources across queues.
	// When false, jobs are protected from fair-share preemption but can still be preempted
	// by higher-priority jobs.
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
