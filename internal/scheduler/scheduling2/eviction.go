package scheduling2

import (
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
	"math"
)

type FairShareEvictor struct{}

func (_ FairShareEvictor) Evict(jobs []*jobdb.Job, shdCtx *context.SchedulingContext, protectedFractionOfFairShare float64) map[string]*jobdb.Job {
	evictedJobs := make(map[string]*jobdb.Job)
	for _, job := range jobs {
		priorityClass := job.PriorityClass()
		if !priorityClass.Preemptible {
			continue
		}
		if qctx, ok := shdCtx.QueueSchedulingContexts[job.Queue()]; ok {
			actualShare := shdCtx.FairnessCostProvider.UnweightedCostFromQueue(qctx)
			fairShare := math.Max(qctx.AdjustedFairShare, qctx.FairShare)
			fractionOfFairShare := actualShare / fairShare
			if fractionOfFairShare > protectedFractionOfFairShare {
				evictedJobs[job.Id()] = job
			}
		}
	}
	return evictedJobs
}

type OversubscribedEvictor struct {
	jobRepository scheduling.JobRepository
}

func (e OversubscribedEvictor) Evict(txn model.NodeDbTransaction) map[string]*jobdb.Job {
	evictedJobs := make(map[string]*jobdb.Job)

	nodeIter := txn.GetAllNodes()
	for node := nodeIter.Next(); node != nil; node = nodeIter.Next() {

		// First check to see if we have any oversubscribed priorities
		// TODO: could this be a method on internaltypes.Node?
		overSubscribedPriorities := make(map[int32]bool)
		for p, rl := range node.AllocatableByPriority {
			if p < 0 {
				// Negative priorities correspond to already evicted jobs.
				continue
			}
			if rl.HasNegativeValues() {
				overSubscribedPriorities[p] = true
			}
		}

		// TODO: this isn't as efficient as the old method as it evicts all jobs on the node, not just those at
		// oversubscribed priority classes.  In order to do the latter, we need to know the priority the job was
		// scheduled at.
		if len(overSubscribedPriorities) > 0 {
			for jobId := range node.AllocatedByJobId {
				if _, ok := node.EvictedJobRunIds[jobId]; !ok {
					job := e.jobRepository.GetById(jobId)
					if job != nil {
						evictedJobs[job.Id()] = job
					}
				}
			}
		}
	}
	return evictedJobs
}
