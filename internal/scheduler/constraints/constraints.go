package constraints

import (
	"math"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/client/queue"
)

const (
	UnschedulableReasonMaximumResourcesScheduled        = "maximum resources scheduled"
	UnschedulableReasonMaximumNumberOfJobsScheduled     = "maximum number of jobs scheduled"
	UnschedulableReasonMaximumNumberOfGangsScheduled    = "maximum number of gangs scheduled"
	UnschedulableReasonMaximumResourcesPerQueueExceeded = "maximum total resources for this queue exceeded"
)

// IsTerminalUnschedulableReason returns true if reason indicates it's not possible to schedule any more jobs in this round.
func IsTerminalUnschedulableReason(reason string) bool {
	if reason == UnschedulableReasonMaximumResourcesScheduled {
		return true
	}
	if reason == UnschedulableReasonMaximumNumberOfJobsScheduled {
		return true
	}
	if reason == UnschedulableReasonMaximumNumberOfGangsScheduled {
		return true
	}
	return false
}

// SchedulingConstraints contains scheduling constraints, e.g., per-queue resource limits.
type SchedulingConstraints struct {
	// Max number of jobs to scheduler per lease jobs call.
	MaximumJobsToSchedule uint
	// Max number of jobs to scheduler per lease jobs call.
	MaximumGangsToSchedule uint
	// Max number of jobs to consider for a queue before giving up.
	MaxQueueLookback uint
	// Jobs leased to this executor must be at least this large.
	// Used, e.g., to avoid scheduling CPU-only jobs onto clusters with GPUs.
	MinimumJobSize schedulerobjects.ResourceList
	// Scheduling constraints by priority class.
	PriorityClassSchedulingConstraintsByPriorityClassName map[string]PriorityClassSchedulingConstraints
	// Scheduling constraints for specific queues.
	// If present for a particular queue, global limits (i.e., PriorityClassSchedulingConstraintsByPriorityClassName)
	// do not apply for that queue.
	QueueSchedulingConstraintsByQueueName map[string]QueueSchedulingConstraints
	// Limits total resources scheduled per invocation.
	MaximumResourcesToSchedule schedulerobjects.ResourceList
}

// QueueSchedulingConstraints contains per-queue scheduling constraints.
type QueueSchedulingConstraints struct {
	// Scheduling constraints by priority class.
	PriorityClassSchedulingConstraintsByPriorityClassName map[string]PriorityClassSchedulingConstraints
}

// PriorityClassSchedulingConstraints contains scheduling constraints that apply to jobs of a specific priority class.
type PriorityClassSchedulingConstraints struct {
	PriorityClassName string
	// Limits total resources allocated to jobs of this priority class per queue.
	MaximumResourcesPerQueue schedulerobjects.ResourceList
}

func NewSchedulingConstraints(
	pool string,
	totalResources schedulerobjects.ResourceList,
	minimumJobSize schedulerobjects.ResourceList,
	config configuration.SchedulingConfig,
	queues []queue.Queue,
) SchedulingConstraints {
	priorityClassSchedulingConstraintsByPriorityClassName := make(map[string]PriorityClassSchedulingConstraints, len(config.Preemption.PriorityClasses))
	for name, priorityClass := range config.Preemption.PriorityClasses {
		maximumResourceFractionPerQueue := priorityClass.MaximumResourceFractionPerQueue
		if m, ok := priorityClass.MaximumResourceFractionPerQueueByPool[pool]; ok {
			// Use pool-specific config is available.
			maximumResourceFractionPerQueue = m
		}
		priorityClassSchedulingConstraintsByPriorityClassName[name] = PriorityClassSchedulingConstraints{
			PriorityClassName:        name,
			MaximumResourcesPerQueue: absoluteFromRelativeLimits(totalResources, maximumResourceFractionPerQueue),
		}
	}

	queueSchedulingConstraintsByQueueName := make(map[string]QueueSchedulingConstraints, len(queues))
	for _, queue := range queues {
		priorityClassSchedulingConstraintsByPriorityClassNameForQueue := make(map[string]PriorityClassSchedulingConstraints, len(queue.ResourceLimitsByPriorityClassName))
		for name, priorityClassResourceLimits := range queue.ResourceLimitsByPriorityClassName {
			maximumResourceFraction := priorityClassResourceLimits.MaximumResourceFraction
			if m, ok := priorityClassResourceLimits.MaximumResourceFractionByPool[pool]; ok {
				// Use pool-specific config is available.
				maximumResourceFraction = m.MaximumResourceFraction
			}
			priorityClassSchedulingConstraintsByPriorityClassName[name] = PriorityClassSchedulingConstraints{
				PriorityClassName:        name,
				MaximumResourcesPerQueue: absoluteFromRelativeLimits(totalResources, maximumResourceFraction),
			}
		}
		if len(priorityClassSchedulingConstraintsByPriorityClassNameForQueue) > 0 {
			queueSchedulingConstraintsByQueueName[queue.Name] = QueueSchedulingConstraints{
				PriorityClassSchedulingConstraintsByPriorityClassName: priorityClassSchedulingConstraintsByPriorityClassNameForQueue,
			}
		}
	}

	maximumResourceFractionToSchedule := config.MaximumResourceFractionToSchedule
	if m, ok := config.MaximumResourceFractionToScheduleByPool[pool]; ok {
		// Use pool-specific config is available.
		maximumResourceFractionToSchedule = m
	}
	return SchedulingConstraints{
		MaximumJobsToSchedule:      config.MaximumJobsToSchedule,
		MaximumGangsToSchedule:     config.MaximumGangsToSchedule,
		MaxQueueLookback:           config.MaxQueueLookback,
		MinimumJobSize:             minimumJobSize,
		MaximumResourcesToSchedule: absoluteFromRelativeLimits(totalResources, maximumResourceFractionToSchedule),
		PriorityClassSchedulingConstraintsByPriorityClassName: priorityClassSchedulingConstraintsByPriorityClassName,
		QueueSchedulingConstraintsByQueueName:                 queueSchedulingConstraintsByQueueName,
	}
}

func absoluteFromRelativeLimits(totalResources schedulerobjects.ResourceList, relativeLimits map[string]float64) schedulerobjects.ResourceList {
	absoluteLimits := schedulerobjects.NewResourceList(len(relativeLimits))
	for t, f := range relativeLimits {
		absoluteLimits.Set(t, ScaleQuantity(totalResources.Get(t).DeepCopy(), f))
	}
	return absoluteLimits
}

func (constraints *SchedulingConstraints) CheckRoundConstraints(sctx *schedulercontext.SchedulingContext) (bool, string, error) {
	// MaximumJobsToSchedule check.
	if constraints.MaximumJobsToSchedule != 0 && sctx.NumScheduledJobs == int(constraints.MaximumJobsToSchedule) {
		return false, UnschedulableReasonMaximumNumberOfJobsScheduled, nil
	}

	// MaximumGangsToSchedule check.
	if constraints.MaximumGangsToSchedule != 0 && sctx.NumScheduledGangs == int(constraints.MaximumGangsToSchedule) {
		return false, UnschedulableReasonMaximumNumberOfGangsScheduled, nil
	}

	// MaximumResourcesToSchedule check.
	if !sctx.ScheduledResources.IsStrictlyLessOrEqual(constraints.MaximumResourcesToSchedule) {
		return false, UnschedulableReasonMaximumResourcesScheduled, nil
	}
	return true, "", nil
}

func (constraints *SchedulingConstraints) CheckPerQueueAndPriorityClassConstraints(
	sctx *schedulercontext.SchedulingContext,
	queue string,
	priorityClassName string,
) (bool, string, error) {
	qctx := sctx.QueueSchedulingContexts[queue]
	if qctx == nil {
		return false, "", errors.Errorf("no QueueSchedulingContext for queue %s", queue)
	}
	if queueConstraint, ok := constraints.QueueSchedulingConstraintsByQueueName[queue]; ok {
		if priorityClassConstraint, ok := queueConstraint.PriorityClassSchedulingConstraintsByPriorityClassName[priorityClassName]; ok {
			if !qctx.AllocatedByPriorityClass[priorityClassName].IsStrictlyLessOrEqual(priorityClassConstraint.MaximumResourcesPerQueue) {
				return false, UnschedulableReasonMaximumResourcesPerQueueExceeded, nil
			}
		}
	} else {
		if priorityClassConstraint, ok := constraints.PriorityClassSchedulingConstraintsByPriorityClassName[priorityClassName]; ok {
			if !qctx.AllocatedByPriorityClass[priorityClassName].IsStrictlyLessOrEqual(priorityClassConstraint.MaximumResourcesPerQueue) {
				return false, UnschedulableReasonMaximumResourcesPerQueueExceeded, nil
			}
		}
	}
	return true, "", nil
}

// ScaleQuantity scales q in-place by a factor f.
// This functions overflows for quantities the milli value of which can't be expressed as an int64.
// E.g., 1Pi is ok, but not 10Pi.
func ScaleQuantity(q resource.Quantity, f float64) resource.Quantity {
	q.SetMilli(int64(math.Round(float64(q.MilliValue()) * f)))
	return q
}
