package scheduling2

import (
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
	"golang.org/x/exp/maps"
)

// A QueueScheduler can schedule all the jobs in a given jobQueue.
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

// Schedule will attempt to schedule all jobs in the given jobQueue. It may choose to evict any jobs in the
// provided evictedJobs map in order to achieve this.
func (s *QueueScheduler) Schedule(schedCtx *context.SchedulingContext, jobQueue model.JobQueue, evictedJobs map[string]*context.JobSchedulingContext) error {

	for gctx := jobQueue.Next(); gctx != nil; gctx = jobQueue.Next() {

		// Verify that we haven't hit any round constraints
		ok, _, err := s.constraints.CheckRoundConstraints(schedCtx)
		if err != nil || !ok {
			return err
		}

		// Verify that this job doesn't break any constraints
		// TODO: we need to confirm whether the constraint check is fatal for the queue
		ok, _, err = s.constraints.CheckConstraints(schedCtx, gctx)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		// Try and assign node
		// TODO:  maps.Values(evictedJobs) is likely to be inefficient here. Ideally we would use a data structure such
		// as a LinkedHashMap
		result, err := s.nodeAssigner.AssignNodesForGang(gctx, maps.Values(evictedJobs))
		if err != nil {
			return err
		}

		// If we scheduled a job then update the accounting
		if result.Scheduled {
			jobQueue.UpdateQueueCost(gctx.Queue, gctx.TotalResourceRequests)
			schedCtx.NumScheduledGangs++
			for _, jobCtx := range gctx.JobSchedulingContexts {
				schedCtx.NumScheduledJobs++
				delete(evictedJobs, jobCtx.JobId) // once we've scheduled a job it can no longer be preempted
			}
		}
	}

	return nil
}
