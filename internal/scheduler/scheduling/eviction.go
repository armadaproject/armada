package scheduling

import (
	"fmt"
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/hashicorp/go-memdb"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type Evictor struct {
	jobRepo    JobRepository
	nodeDb     *nodedb.NodeDb
	nodeFilter func(*armadacontext.Context, *internaltypes.Node) bool
	jobFilter  func(*armadacontext.Context, *jobdb.Job) (bool, string)
}

type EvictorResult struct {
	// For all evicted jobs, map from job id to the scheduling context for re-scheduling that job.
	EvictedJctxsByJobId map[string]*schedulercontext.JobSchedulingContext
	// Map from node id to node, containing all nodes on which at least one job was evicted.
	AffectedNodesById map[string]*internaltypes.Node
	// For each evicted job, maps the id of the job to the id of the node it was evicted from.
	NodeIdByJobId map[string]string
	// For each node, is it possible to preempt all jobs on the node, and, if not, why?
	NodePreemptiblityStats []NodePreemptiblityStats
}

type NodePreemptiblityStats struct {
	// Node name
	NodeName string
	// Cluster
	Cluster string
	// Reporting node type
	NodeType string
	// If you can't preempt all jobs on this node, the reason why not. Otherwise empty.
	Reason string
}

type queueChecker interface {
	QueueContextExists(job *jobdb.Job) bool
}

type EvictorPerQueueStats struct {
	EvictedJobCount  int
	EvictedResources internaltypes.ResourceList
}

func (er *EvictorResult) GetStatsPerQueue() map[string]EvictorPerQueueStats {
	statsPerQueue := map[string]EvictorPerQueueStats{}
	for _, jctx := range er.EvictedJctxsByJobId {
		queue := jctx.Job.Queue()
		stats := statsPerQueue[queue]
		stats.EvictedJobCount++
		stats.EvictedResources = stats.EvictedResources.Add(jctx.Job.KubernetesResourceRequirements())
		statsPerQueue[queue] = stats
	}

	return statsPerQueue
}

func (er *EvictorResult) SummaryString() string {
	statsPerQueue := er.GetStatsPerQueue()

	return fmt.Sprintf("%v", armadamaps.MapValues(statsPerQueue, func(s EvictorPerQueueStats) string {
		return fmt.Sprintf("{evictedJobCount=%d, evictedResources={%s}}", s.EvictedJobCount, s.EvictedResources.String())
	}))
}

func NewNodeEvictor(
	jobRepo JobRepository,
	nodeDb *nodedb.NodeDb,
	jobFilter func(*armadacontext.Context, *jobdb.Job) (bool, string),
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
	notEvictReason string,
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
		jobFilter: func(_ *armadacontext.Context, job *jobdb.Job) (bool, string) {
			shouldEvict := jobIdsToEvict[job.Id()]
			r := ""
			if !shouldEvict {
				r = notEvictReason
			}
			return shouldEvict, r
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
		jobFilter: func(ctx *armadacontext.Context, job *jobdb.Job) (bool, string) {
			if !queueChecker.QueueContextExists(job) {
				ctx.Warnf("No queue context found for job %s.  This job cannot be evicted", job.Id())
				return false, "invalid_queue"
			}
			priorityClass := job.PriorityClass()
			if !priorityClass.Preemptible {
				return false, "job_not_preemptible"
			}
			priority, ok := nodeDb.GetScheduledAtPriority(job.Id())
			if !ok {
				ctx.Warnf("can't evict job %s: not mapped to a priority", job.Id())
				return false, "invalid_priority"
			}
			overSubscribed := overSubscribedPriorities[priority]
			r := ""
			if overSubscribed {
				r = "over_subscribed"
			}
			return overSubscribed, r
		},
	}
}

// Evict removes jobs from nodes, returning all affected jobs and nodes.
// Any node for which nodeFilter returns false is skipped.
// Any job for which jobFilter returns true is evicted (if the node was not skipped).
// If a job was evicted from a node, postEvictFunc is called with the corresponding job and node.
func (evi *Evictor) Evict(ctx *armadacontext.Context, nodeDbTxn *memdb.Txn) (*EvictorResult, error) {
	evictedJctxsByJobId := make(map[string]*schedulercontext.JobSchedulingContext)
	affectedNodesById := make(map[string]*internaltypes.Node)
	nodeIdByJobId := make(map[string]string)
	nodePreemptiblityStats := []NodePreemptiblityStats{}

	it, err := nodedb.NewNodesIterator(nodeDbTxn)
	if err != nil {
		return nil, err
	}

	for node := it.NextNode(); node != nil; node = it.NextNode() {
		if evi.nodeFilter != nil && !evi.nodeFilter(ctx, node) {
			continue
		}
		jobs := make([]*jobdb.Job, 0, len(node.AllocatedByJobId))
		reasons := map[string]bool{}
		for jobId := range node.AllocatedByJobId {
			if _, ok := node.EvictedJobRunIds[jobId]; !ok {
				job := evi.jobRepo.GetById(jobId)
				if job != nil {
					shouldEvict, dontEvictReason := evi.jobFilter(ctx, job)
					if shouldEvict {
						jobs = append(jobs, job)
					}
					if dontEvictReason != "" {
						reasons[dontEvictReason] = true
					}
				}
			}
		}
		nodePreemptiblityStats = append(nodePreemptiblityStats, makeNodePreemptiblityStats(node, reasons))
		node, err := evi.nodeDb.EvictJobsFromNode(jobs, node)
		if err != nil {
			return nil, err
		}

		for _, job := range jobs {
			// Create a scheduling context for the attempt to re-schedule the job, and:
			// 1. Mark the job as evicted. This ensures total scheduled resources is calculated correctly.
			// 2. Add a node selector ensuring the job can only be re-scheduled onto the node it was evicted from.
			// 3. Add tolerations for all taints on the node. This to ensure that:
			//    - Adding taints to a node doesn't cause jobs already running on the node to be preempted.
			//    - Jobs scheduled as away jobs have the necessary tolerations to be re-scheduled.
			// TODO(albin): We can remove the checkOnlyDynamicRequirements flag in the nodeDb now that we've added the tolerations.
			jctx := schedulercontext.JobSchedulingContextFromJob(job)
			jctx.IsEvicted = true
			jctx.SetAssignedNode(node)
			evictedJctxsByJobId[job.Id()] = jctx
			jctx.AdditionalTolerations = append(jctx.AdditionalTolerations, node.GetTolerationsForTaints()...)

			nodeIdByJobId[job.Id()] = node.GetId()
		}
		if len(jobs) > 0 {
			affectedNodesById[node.GetId()] = node
		}
	}
	result := &EvictorResult{
		EvictedJctxsByJobId:    evictedJctxsByJobId,
		AffectedNodesById:      affectedNodesById,
		NodeIdByJobId:          nodeIdByJobId,
		NodePreemptiblityStats: nodePreemptiblityStats,
	}

	return result, nil
}

func makeNodePreemptiblityStats(node *internaltypes.Node, reasons map[string]bool) NodePreemptiblityStats {
	reasonsList := maps.Keys(reasons)
	slices.Sort(reasonsList)
	return NodePreemptiblityStats{
		NodeName: node.GetName(),
		Cluster:  node.GetExecutor(),
		NodeType: node.GetReportingNodeType(),
		Reason:   strings.Join(reasonsList, ","),
	}
}
