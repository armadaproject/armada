package scheduling2

import (
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
	"golang.org/x/exp/maps"
)

type QueueScheduler struct {
	constraints  schedulerconstraints.SchedulingConstraints
	nodeAssigner model.NodeAssigner
}

func NewQueueScheduler(constraints schedulerconstraints.SchedulingConstraints, nodeAssigner model.NodeAssigner) *QueueScheduler {
	return &QueueScheduler{
		constraints:  constraints,
		nodeAssigner: nodeAssigner,
	}
}

func (s *QueueScheduler) Schedule(schedCtx *context.SchedulingContext, jobQueue model.JobQueue, evictedJobs map[string]*context.JobSchedulingContext) error {

	for gctx := jobQueue.Next(); gctx != nil; gctx = jobQueue.Next() {

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
		result, err := s.nodeAssigner.AssignNodesForGang(gctx, maps.Values(evictedJobs))
		if err != nil {
			return err
		}

		// If we scheduled a job then update the accounting
		if result.Scheduled {
			jobQueue.UpdateQueueCost()
			schedCtx.NumScheduledGangs++
			for _, jobCtx := range gctx.JobSchedulingContexts {
				schedCtx.NumScheduledJobs++
				delete(evictedJobs, jobCtx.JobId)
			}
		}
	}

	return nil
}
