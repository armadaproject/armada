package scheduling

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/hashicorp/go-memdb"
)

type Evictor struct {
	jobRepo    JobRepository
	nodeDb     *nodedb.NodeDb
	nodeFilter func(*armadacontext.Context, *internaltypes.Node) bool
	jobFilter  func(*armadacontext.Context, *jobdb.Job) bool
}

type EvictorResult struct {
	// For all evicted jobs, map from job id to the scheduling context for re-scheduling that job.
	EvictedJctxsByJobId map[string]*schedulercontext.JobSchedulingContext
	// Map from node id to node, containing all nodes on which at least one job was evicted.
	AffectedNodesById map[string]*internaltypes.Node
	// For each evicted job, maps the id of the job to the id of the node it was evicted from.
	NodeIdByJobId map[string]string
}

type queueChecker interface {
	QueueContextExists(job *jobdb.Job) bool
}

func (er *EvictorResult) SummaryString() string {
	type queueStats struct {
		evictedJobCount  int
		evictedResources internaltypes.ResourceList
	}
	statsPerQueue := map[string]queueStats{}
	for _, jctx := range er.EvictedJctxsByJobId {
		queue := jctx.Job.Queue()
		stats := statsPerQueue[queue]
		stats.evictedJobCount++
		stats.evictedResources = stats.evictedResources.Add(jctx.Job.KubernetesResourceRequirements())
		statsPerQueue[queue] = stats
	}
	return fmt.Sprintf("%v", armadamaps.MapValues(statsPerQueue, func(s queueStats) string {
		return fmt.Sprintf("{evictedJobCount=%d, evictedResources={%s}}", s.evictedJobCount, s.evictedResources.String())
	}))
}

func NewNodeEvictor(
	jobRepo JobRepository,
	nodeDb *nodedb.NodeDb,
	jobFilter func(*armadacontext.Context, *jobdb.Job) bool,
) *Evictor {
	return &Evictor{
		jobRepo: jobRepo,
		nodeDb:  nodeDb,
		nodeFilter: func(_ *armadacontext.Context, node *internaltypes.Node) bool {
			return len(node.AllocatedByJobId) > 0
		},
		jobFilter: jobFilter,
	}
}

// NewFilteredEvictor returns a new evictor that evicts all jobs for which jobIdsToEvict[jobId] is true
// on nodes for which nodeIdsToEvict[nodeId] is true.
func NewFilteredEvictor(
	jobRepo JobRepository,
	nodeDb *nodedb.NodeDb,
	nodeIdsToEvict map[string]bool,
	jobIdsToEvict map[string]bool,
) *Evictor {
	if len(nodeIdsToEvict) == 0 || len(jobIdsToEvict) == 0 {
		return nil
	}
	return &Evictor{
		jobRepo: jobRepo,
		nodeDb:  nodeDb,
		nodeFilter: func(_ *armadacontext.Context, node *internaltypes.Node) bool {
			shouldEvict := nodeIdsToEvict[node.GetId()]
			return shouldEvict
		},
		jobFilter: func(_ *armadacontext.Context, job *jobdb.Job) bool {
			shouldEvict := jobIdsToEvict[job.Id()]
			return shouldEvict
		},
	}
}

// NewOversubscribedEvictor returns a new evictor that
// for each node evicts all preemptible jobs of a priority class for which at least one job could not be scheduled
func NewOversubscribedEvictor(
	queueChecker queueChecker,
	jobRepo JobRepository,
	nodeDb *nodedb.NodeDb,
) *Evictor {
	// Populating overSubscribedPriorities relies on
	// - nodeFilter being called once before all calls to jobFilter and
	// - jobFilter being called for all jobs on that node before moving on to another node.
	var overSubscribedPriorities map[int32]bool
	return &Evictor{
		jobRepo: jobRepo,
		nodeDb:  nodeDb,
		nodeFilter: func(_ *armadacontext.Context, node *internaltypes.Node) bool {
			overSubscribedPriorities = make(map[int32]bool)
			for p, rl := range node.AllocatableByPriority {
				if p < 0 {
					// Negative priorities correspond to already evicted jobs.
					continue
				}
				if rl.HasNegativeValues() {
					overSubscribedPriorities[p] = true
				}
			}
			return len(overSubscribedPriorities) > 0
		},
		jobFilter: func(ctx *armadacontext.Context, job *jobdb.Job) bool {
			if !queueChecker.QueueContextExists(job) {
				ctx.Warnf("No queue context found for job %s.  This job cannot be evicted", job.Id())
				return false
			}
			priorityClass := job.PriorityClass()
			if !priorityClass.Preemptible {
				return false
			}
			priority, ok := nodeDb.GetScheduledAtPriority(job.Id())
			if !ok {
				ctx.Warnf("can't evict job %s: not mapped to a priority", job.Id())
				return false
			}
			return overSubscribedPriorities[priority]
		},
	}
}

// Evict removes jobs from nodes, returning all affected jobs and nodes.
// Any node for which nodeFilter returns false is skipped.
// Any job for which jobFilter returns true is evicted (if the node was not skipped).
// If a job was evicted from a node, postEvictFunc is called with the corresponding job and node.
func (evi *Evictor) Evict(ctx *armadacontext.Context, nodeDbTxn *memdb.Txn) (*EvictorResult, error) {
	var jobFilter func(job *jobdb.Job) bool
	if evi.jobFilter != nil {
		jobFilter = func(job *jobdb.Job) bool { return evi.jobFilter(ctx, job) }
	}
	evictedJctxsByJobId := make(map[string]*schedulercontext.JobSchedulingContext)
	affectedNodesById := make(map[string]*internaltypes.Node)
	nodeIdByJobId := make(map[string]string)

	it, err := nodedb.NewNodesIterator(nodeDbTxn)
	if err != nil {
		return nil, err
	}

	for node := it.NextNode(); node != nil; node = it.NextNode() {
		if evi.nodeFilter != nil && !evi.nodeFilter(ctx, node) {
			continue
		}
		jobs := make([]*jobdb.Job, 0, len(node.AllocatedByJobId))
		for jobId := range node.AllocatedByJobId {
			if _, ok := node.EvictedJobRunIds[jobId]; !ok {
				job := evi.jobRepo.GetById(jobId)
				if job != nil {
					jobs = append(jobs, job)
				}

			}
		}
		evictedJobs, node, err := evi.nodeDb.EvictJobsFromNode(jobFilter, jobs, node)
		if err != nil {
			return nil, err
		}

		for _, job := range evictedJobs {
			// Create a scheduling context for the attempt to re-schedule the job, and:
			// 1. Mark the job as evicted. This ensures total scheduled resources is calculated correctly.
			// 2. Add a node selector ensuring the job can only be re-scheduled onto the node it was evicted from.
			// 3. Add tolerations for all taints on the node. This to ensure that:
			//    - Adding taints to a node doesn't cause jobs already running on the node to be preempted.
			//    - Jobs scheduled as away jobs have the necessary tolerations to be re-scheduled.
			// TODO(albin): We can remove the checkOnlyDynamicRequirements flag in the nodeDb now that we've added the tolerations.
			jctx := schedulercontext.JobSchedulingContextFromJob(job)
			jctx.IsEvicted = true
			jctx.SetAssignedNodeId(node.GetId())
			evictedJctxsByJobId[job.Id()] = jctx
			jctx.AdditionalTolerations = append(jctx.AdditionalTolerations, node.GetTolerationsForTaints()...)

			nodeIdByJobId[job.Id()] = node.GetId()
		}
		if len(evictedJobs) > 0 {
			affectedNodesById[node.GetId()] = node
		}
	}
	result := &EvictorResult{
		EvictedJctxsByJobId: evictedJctxsByJobId,
		AffectedNodesById:   affectedNodesById,
		NodeIdByJobId:       nodeIdByJobId,
	}

	return result, nil
}
