package scheduling2

import (
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type Scheduler struct {
	jobQueue     model.JobQueue
	constraints  schedulerconstraints.SchedulingConstraints
	nodeAssigner model.NodeAssigner
}

// TODO:
// * How do we work out what jobs to evict
// * How do we deal with jobs that have been preempted mid cycle
//	* Callback to node assigner to say what jobs have been scheduled?
// * How do we deal with jobs that are preempted at the end
// * How do we deal with ensuring we schedule all the evicted jobs
//   - evicted iterator never times out- that means we don't have to worry about that
//   - global constraints we need to work out if terminal.  If so set only yield evicted
//   - queue constraints same but need to set only yield evicted by queue
//
// * How do we ensure that we preserve the scheduling info

func (s *Scheduler) Schedule(schedCtx *context.SchedulingContext) error {

	for gctx := s.jobQueue.Next(); gctx != nil; gctx = s.jobQueue.Next() {

		// Verify that we haven't hit any round constraints
		ok, _, err := s.constraints.CheckRoundConstraints(schedCtx)
		if err != nil || !ok {
			return err
		}

		// Verify that this job doesn't break any constraints
		ok, _, err = s.constraints.CheckConstraints(schedCtx, gctx)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		// Try and assign node
		result, err := s.nodeAssigner.AssignNode(gctx)
		if err != nil {
			return err
		}

		// If we scheduled a job then update the accounting
		if result.Scheduled {
			s.jobQueue.UpdateQueueCost()
			schedCtx.NumScheduledGangs++
		}
	}

	return nil
}
