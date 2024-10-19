package scheduling

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
)

// PreemptingQueueScheduler is a scheduler that makes a unified decisions on which jobs to preempt and schedule.
// Uses QueueScheduler as a building block.
type PreemptingQueueScheduler struct {
	schedulingContext            *schedulercontext.SchedulingContext
	constraints                  schedulerconstraints.SchedulingConstraints
	floatingResourceTypes        *floatingresources.FloatingResourceTypes
	protectedFractionOfFairShare float64
	jobRepo                      JobRepository
	nodeDb                       *nodedb.NodeDb
	// Maps job ids to the id of the node the job is associated with.
	// For scheduled or running jobs, that is the node the job is assigned to.
	// For preempted jobs, that is the node the job was preempted from.
	nodeIdByJobId map[string]string
	// Maps gang ids to the ids of jobs in that gang.
	jobIdsByGangId map[string]map[string]bool
	// Maps job ids of gang jobs to the id of that gang.
	gangIdByJobId map[string]string
}

func NewPreemptingQueueScheduler(
	sctx *schedulercontext.SchedulingContext,
	constraints schedulerconstraints.SchedulingConstraints,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
	protectedFractionOfFairShare float64,
	jobRepo JobRepository,
	nodeDb *nodedb.NodeDb,
	initialNodeIdByJobId map[string]string,
	initialJobIdsByGangId map[string]map[string]bool,
	initialGangIdByJobId map[string]string,
) *PreemptingQueueScheduler {
	if initialNodeIdByJobId == nil {
		initialNodeIdByJobId = make(map[string]string)
	}
	if initialJobIdsByGangId == nil {
		initialJobIdsByGangId = make(map[string]map[string]bool)
	}
	if initialGangIdByJobId == nil {
		initialGangIdByJobId = make(map[string]string)
	}
	initialJobIdsByGangId = maps.Clone(initialJobIdsByGangId)
	for gangId, jobIds := range initialJobIdsByGangId {
		initialJobIdsByGangId[gangId] = maps.Clone(jobIds)
	}
	return &PreemptingQueueScheduler{
		schedulingContext:            sctx,
		constraints:                  constraints,
		floatingResourceTypes:        floatingResourceTypes,
		protectedFractionOfFairShare: protectedFractionOfFairShare,
		jobRepo:                      jobRepo,
		nodeDb:                       nodeDb,
		nodeIdByJobId:                maps.Clone(initialNodeIdByJobId),
		jobIdsByGangId:               initialJobIdsByGangId,
		gangIdByJobId:                maps.Clone(initialGangIdByJobId),
	}
}

// Schedule
// - preempts jobs belonging to queues with total allocation above their fair share and
// - schedules new jobs belonging to queues with total allocation less than their fair share.
func (sch *PreemptingQueueScheduler) Schedule(ctx *armadacontext.Context) (*SchedulerResult, error) {
	defer func() {
		sch.schedulingContext.Finished = time.Now()
	}()

	preemptedJobsById := make(map[string]*schedulercontext.JobSchedulingContext)
	scheduledJobsById := make(map[string]*schedulercontext.JobSchedulingContext)

	// Evict preemptible jobs.
	ctx.WithField("stage", "scheduling-algo").Infof("Evicting preemptible jobs")
	evictorResult, inMemoryJobRepo, err := sch.evict(
		armadacontext.WithLogField(ctx, "stage", "evict for resource balancing"),
		NewNodeEvictor(
			sch.jobRepo,
			sch.nodeDb,
			func(ctx *armadacontext.Context, job *jobdb.Job) bool {
				if job.LatestRun().Pool() != sch.schedulingContext.Pool {
					return false
				}
				if !sch.schedulingContext.QueueContextExists(job) {
					ctx.Warnf("No queue context found for job %s.  This job cannot be evicted", job.Id())
					return false
				}
				priorityClass := job.PriorityClass()
				if !priorityClass.Preemptible {
					return false
				}
				if job.Annotations() == nil {
					ctx.Errorf("can't evict job %s: annotations not initialised", job.Id())
					return false
				}
				if job.NodeSelector() == nil {
					ctx.Errorf("can't evict job %s: nodeSelector not initialised", job.Id())
					return false
				}
				if qctx, ok := sch.schedulingContext.QueueSchedulingContexts[job.Queue()]; ok {
					actualShare := sch.schedulingContext.FairnessCostProvider.UnweightedCostFromQueue(qctx)
					fairShare := math.Max(qctx.AdjustedFairShare, qctx.FairShare)
					fractionOfFairShare := actualShare / fairShare
					if fractionOfFairShare <= sch.protectedFractionOfFairShare {
						return false
					}
				}
				return true
			},
		),
	)
	if err != nil {
		return nil, err
	}
	ctx.WithField("stage", "scheduling-algo").Info("Finished evicting preemptible jobs")
	for _, jctx := range evictorResult.EvictedJctxsByJobId {
		preemptedJobsById[jctx.JobId] = jctx
	}
	maps.Copy(sch.nodeIdByJobId, evictorResult.NodeIdByJobId)

	// Re-schedule evicted jobs/schedule new jobs.
	ctx.WithField("stage", "scheduling-algo").Info("Performing initial scheduling of jobs onto nodes")
	schedulerResult, err := sch.schedule(
		armadacontext.WithLogField(ctx, "stage", "re-schedule after balancing eviction"),
		inMemoryJobRepo,
		sch.jobRepo,
		false,
		false,
	)
	if err != nil {
		return nil, err
	}
	ctx.WithField("stage", "scheduling-algo").Info("Finished initial scheduling of jobs onto nodes")
	for _, jctx := range schedulerResult.ScheduledJobs {
		if _, ok := preemptedJobsById[jctx.JobId]; ok {
			delete(preemptedJobsById, jctx.JobId)
		} else {
			scheduledJobsById[jctx.JobId] = jctx
		}
	}
	maps.Copy(sch.nodeIdByJobId, schedulerResult.NodeIdByJobId)

	// Evict jobs on oversubscribed nodes.
	ctx.WithField("stage", "scheduling-algo").Info("Evicting jobs from oversubscribed nodes")
	evictorResult, inMemoryJobRepo, err = sch.evict(
		armadacontext.WithLogField(ctx, "stage", "evict oversubscribed"),
		NewOversubscribedEvictor(
			sch.schedulingContext,
			sch.jobRepo,
			sch.nodeDb,
		),
	)
	if err != nil {
		return nil, err
	}
	ctx.WithField("stage", "scheduling-algo").Info("Finished evicting jobs from oversubscribed nodes")
	scheduledAndEvictedJobsById := armadamaps.FilterKeys(
		scheduledJobsById,
		func(jobId string) bool {
			_, ok := evictorResult.EvictedJctxsByJobId[jobId]
			return ok
		},
	)
	for jobId, jctx := range evictorResult.EvictedJctxsByJobId {
		if _, ok := scheduledJobsById[jobId]; ok {
			delete(scheduledJobsById, jobId)
		} else {
			preemptedJobsById[jobId] = jctx
		}
	}
	maps.Copy(sch.nodeIdByJobId, evictorResult.NodeIdByJobId)

	// Re-schedule evicted jobs/schedule new jobs.
	// Only necessary if a non-zero number of jobs were evicted.
	if len(evictorResult.EvictedJctxsByJobId) > 0 {
		ctx.WithField("stage", "scheduling-algo").Info("Performing second scheduling ")
		schedulerResult, err = sch.schedule(
			armadacontext.WithLogField(ctx, "stage", "schedule after oversubscribed eviction"),
			inMemoryJobRepo,
			// Only evicted jobs should be scheduled in this round.
			nil,
			true, // Since no new jobs are considered in this round, the scheduling key check brings no benefit.
			true, // when choosing which queue to consider use the priority class of the next job
		)
		if err != nil {
			return nil, err
		}
		ctx.WithField("stage", "scheduling-algo").Info("Finished second scheduling pass")
		for _, jctx := range schedulerResult.ScheduledJobs {
			if _, ok := preemptedJobsById[jctx.JobId]; ok {
				delete(preemptedJobsById, jctx.JobId)
			} else {
				scheduledJobsById[jctx.JobId] = jctx
			}
			delete(scheduledAndEvictedJobsById, jctx.JobId)
		}
		maps.Copy(sch.nodeIdByJobId, schedulerResult.NodeIdByJobId)
	}

	preemptedJobs := maps.Values(preemptedJobsById)
	scheduledJobs := maps.Values(scheduledJobsById)
	ctx.WithField("stage", "scheduling-algo").Infof("Unbinding %d preempted and %d evicted jobs", len(preemptedJobs), len(maps.Values(scheduledAndEvictedJobsById)))
	if err := sch.unbindJobs(append(
		slices.Clone(preemptedJobs),
		maps.Values(scheduledAndEvictedJobsById)...),
	); err != nil {
		return nil, err
	}
	ctx.WithField("stage", "scheduling-algo").Infof("Finished unbinding preempted and evicted jobs")

	schedulercontext.PrintJobSummary(ctx, "Preempting running jobs;", preemptedJobs)
	schedulercontext.PrintJobSummary(ctx, "Scheduling new jobs;", scheduledJobs)
	// TODO: Show failed jobs.

	return &SchedulerResult{
		PreemptedJobs:      preemptedJobs,
		ScheduledJobs:      scheduledJobs,
		NodeIdByJobId:      sch.nodeIdByJobId,
		SchedulingContexts: []*schedulercontext.SchedulingContext{sch.schedulingContext},
	}, nil
}

func (sch *PreemptingQueueScheduler) evict(ctx *armadacontext.Context, evictor *Evictor) (*EvictorResult, *InMemoryJobRepository, error) {
	if evictor == nil {
		return &EvictorResult{}, NewInMemoryJobRepository(sch.schedulingContext.Pool), nil
	}
	txn := sch.nodeDb.Txn(true)
	defer txn.Abort()

	// Evict using the provided evictor.
	result, err := evictor.Evict(ctx, txn)
	if err != nil {
		return nil, nil, err
	}

	ctx.Infof("Evicting for pool %s (most may get re-scheduled this cycle so they won't necessarily be preempted) %s", sch.schedulingContext.Pool, result.SummaryString())

	if err := sch.nodeDb.UpsertManyWithTxn(txn, maps.Values(result.AffectedNodesById)); err != nil {
		return nil, nil, err
	}

	// Evict any remaining jobs in partially evicted gangs.
	// Add any changes to the result of the first evictor.
	gangEvictorResult, err := sch.evictGangs(ctx, txn, result)
	if err != nil {
		return nil, nil, err
	}
	if err := sch.nodeDb.UpsertManyWithTxn(txn, maps.Values(gangEvictorResult.AffectedNodesById)); err != nil {
		return nil, nil, err
	}
	maps.Copy(result.EvictedJctxsByJobId, gangEvictorResult.EvictedJctxsByJobId)
	maps.Copy(result.AffectedNodesById, gangEvictorResult.AffectedNodesById)
	maps.Copy(result.NodeIdByJobId, gangEvictorResult.NodeIdByJobId)

	sch.setEvictedGangCardinality(result)
	evictedJctxs := maps.Values(result.EvictedJctxsByJobId)
	for _, jctx := range evictedJctxs {
		if _, err := sch.schedulingContext.EvictJob(jctx); err != nil {
			return nil, nil, err
		}
	}
	// TODO: Move gang accounting into context.
	if err := sch.updateGangAccounting(evictedJctxs, nil); err != nil {
		return nil, nil, err
	}
	if err := sch.evictionAssertions(result); err != nil {
		return nil, nil, err
	}
	inMemoryJobRepo := NewInMemoryJobRepository(sch.schedulingContext.Pool)
	inMemoryJobRepo.EnqueueMany(evictedJctxs)
	txn.Commit()

	if err := sch.nodeDb.Reset(); err != nil {
		return nil, nil, err
	}
	if err := addEvictedJobsToNodeDb(ctx, sch.schedulingContext, sch.nodeDb, inMemoryJobRepo); err != nil {
		return nil, nil, err
	}
	return result, inMemoryJobRepo, nil
}

// When evicting jobs, gangs may have been partially evicted.
// Here, we evict all jobs in any gang for which at least one job was already evicted.
func (sch *PreemptingQueueScheduler) evictGangs(ctx *armadacontext.Context, txn *memdb.Txn, previousEvictorResult *EvictorResult) (*EvictorResult, error) {
	gangJobIds, gangNodeIds, err := sch.collectIdsForGangEviction(previousEvictorResult)
	if err != nil {
		return nil, err
	}
	gangNodeIds = armadamaps.FilterKeys(
		gangNodeIds,
		// Filter out any nodes already processed.
		// (Just for efficiency; not strictly necessary.)
		// This assumes all gang jobs on these nodes were already evicted.
		func(nodeId string) bool {
			_, ok := previousEvictorResult.AffectedNodesById[nodeId]
			return !ok
		},
	)
	evictor := NewFilteredEvictor(
		sch.jobRepo,
		sch.nodeDb,
		gangNodeIds,
		gangJobIds,
	)
	if evictor == nil {
		// No gangs to evict.
		return &EvictorResult{}, nil
	}

	result, err := evictor.Evict(ctx, txn)
	if err != nil {
		ctx.Infof("Evicting remains of partially evicted gangs for pool %s (most may get re-scheduled this cycle so they won't necessarily be preempted) %s", sch.schedulingContext.Pool, result.SummaryString())
	}

	return result, err
}

// Collect job ids for any gangs that were partially evicted and the ids of nodes those jobs are on.
func (sch *PreemptingQueueScheduler) collectIdsForGangEviction(evictorResult *EvictorResult) (map[string]bool, map[string]bool, error) {
	allGangJobIds := make(map[string]bool)
	gangNodeIds := make(map[string]bool)
	seenGangs := make(map[string]bool)
	for jobId := range evictorResult.EvictedJctxsByJobId {
		gangId, ok := sch.gangIdByJobId[jobId]
		if !ok {
			// Not a gang job.
			continue
		}
		if gangId == "" {
			return nil, nil, errors.Errorf("no gang id found for job %s", jobId)
		}
		if seenGangs[gangId] {
			// Gang already processed.
			continue
		}

		// Look up the gang this job is part of.
		gangJobIds := sch.jobIdsByGangId[gangId]
		if len(gangJobIds) == 0 {
			return nil, nil, errors.Errorf("no jobs found for gang %s", gangId)
		}

		// Collect all job ids part of that gang.
		for gangJobId := range gangJobIds {
			allGangJobIds[gangJobId] = true
			if nodeId, ok := sch.nodeIdByJobId[gangJobId]; !ok {
				return nil, nil, errors.Errorf("no node associated with gang job %s", gangJobId)
			} else if nodeId == "" {
				return nil, nil, errors.Errorf("empty node id associated with with gang job %s", gangJobId)
			} else {
				gangNodeIds[nodeId] = true
			}
		}
		seenGangs[gangId] = true
	}
	return allGangJobIds, gangNodeIds, nil
}

// Some jobs in a gang may have terminated since the gang was scheduled.
// For these gangs, we need to set the gang cardinality to the number of jobs in the gang yet to terminate.
// Otherwise, the evicted gang jobs will not be schedulable, since some gang jobs will be considered missing.
func (sch *PreemptingQueueScheduler) setEvictedGangCardinality(evictorResult *EvictorResult) {
	for _, jctx := range evictorResult.EvictedJctxsByJobId {
		gangId, ok := sch.gangIdByJobId[jctx.Job.Id()]
		if !ok {
			// Not a gang job.
			continue
		}

		// Override cardinality with the number of evicted jobs in this gang.
		jctx.GangInfo.Cardinality = len(sch.jobIdsByGangId[gangId])
	}
	return
}

func (sch *PreemptingQueueScheduler) evictionAssertions(evictorResult *EvictorResult) error {
	for _, qctx := range sch.schedulingContext.QueueSchedulingContexts {
		if !qctx.AllocatedByPriorityClass.IsStrictlyNonNegative() {
			return errors.Errorf("negative allocation for queue %s after eviction: %s", qctx.Queue, qctx.AllocatedByPriorityClass)
		}
	}
	evictedJobIdsByGangId := make(map[string]map[string]bool)
	for jobId, jctx := range evictorResult.EvictedJctxsByJobId {
		if gangId, ok := sch.gangIdByJobId[jobId]; ok {
			if m := evictedJobIdsByGangId[gangId]; m != nil {
				m[jobId] = true
			} else {
				evictedJobIdsByGangId[gangId] = map[string]bool{jobId: true}
			}
		}
		if !jctx.IsEvicted {
			return errors.New("evicted job %s is not marked as such")
		}
		if nodeId := jctx.GetAssignedNodeId(); nodeId != "" {
			if _, ok := evictorResult.AffectedNodesById[nodeId]; !ok {
				return errors.Errorf("node id %s targeted by job %s is not marked as affected", nodeId, jobId)
			}
		} else {
			return errors.Errorf("evicted job %s is missing target node id selector: job nodeSelector %v", jobId, jctx.AdditionalNodeSelectors)
		}
	}
	for gangId, evictedGangJobIds := range evictedJobIdsByGangId {
		if !maps.Equal(evictedGangJobIds, sch.jobIdsByGangId[gangId]) {
			return errors.Errorf(
				"gang %s was partially evicted: %d out of %d jobs evicted",
				gangId, len(evictedGangJobIds), len(sch.jobIdsByGangId[gangId]),
			)
		}
	}
	return nil
}

type MinimalQueueRepository struct {
	queues map[string]MinimalQueue
}

func (qr *MinimalQueueRepository) GetQueue(name string) (fairness.Queue, bool) {
	queue, ok := qr.queues[name]
	return queue, ok
}

func NewMinimalQueueRepositoryFromSchedulingContext(sctx *schedulercontext.SchedulingContext) *MinimalQueueRepository {
	queues := make(map[string]MinimalQueue, len(sctx.QueueSchedulingContexts))
	for name, qctx := range sctx.QueueSchedulingContexts {
		queues[name] = MinimalQueue{allocation: qctx.Allocated.DeepCopy(), weight: qctx.Weight}
	}
	return &MinimalQueueRepository{queues: queues}
}

type MinimalQueue struct {
	allocation schedulerobjects.ResourceList
	weight     float64
}

func (q MinimalQueue) GetAllocation() schedulerobjects.ResourceList {
	return q.allocation
}

func (q MinimalQueue) GetWeight() float64 {
	return q.weight
}

// addEvictedJobsToNodeDb adds evicted jobs to the NodeDb.
// Needed to enable the nodeDb accounting for these when preempting.
func addEvictedJobsToNodeDb(_ *armadacontext.Context, sctx *schedulercontext.SchedulingContext, nodeDb *nodedb.NodeDb, inMemoryJobRepo *InMemoryJobRepository) error {
	gangItByQueue := make(map[string]*QueuedGangIterator)
	for _, qctx := range sctx.QueueSchedulingContexts {
		gangItByQueue[qctx.Queue] = NewQueuedGangIterator(
			inMemoryJobRepo.GetJobIterator(qctx.Queue),
		)
	}
	qr := NewMinimalQueueRepositoryFromSchedulingContext(sctx)
	candidateGangIterator, err := NewCandidateGangIterator(sctx.Pool, qr, sctx.FairnessCostProvider, gangItByQueue, false)
	if err != nil {
		return err
	}
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	i := 0
	for {
		if gctx, _, err := candidateGangIterator.Peek(); err != nil {
			return err
		} else if gctx == nil {
			break
		} else {
			for _, jctx := range gctx.JobSchedulingContexts {
				if err := nodeDb.AddEvictedJobSchedulingContextWithTxn(txn, i, jctx); err != nil {
					return err
				}
				i++
			}
			q := qr.queues[gctx.Queue]
			q.allocation.Add(gctx.TotalResourceRequests)
		}
		if err := candidateGangIterator.Clear(); err != nil {
			return err
		}
	}
	txn.Commit()
	return nil
}

func (sch *PreemptingQueueScheduler) schedule(ctx *armadacontext.Context, inMemoryJobRepo *InMemoryJobRepository, jobRepo JobRepository, skipUnsuccessfulSchedulingKeyCheck bool, considerPriorityCLassPriority bool) (*SchedulerResult, error) {
	jobIteratorByQueue := make(map[string]JobContextIterator)
	for _, qctx := range sch.schedulingContext.QueueSchedulingContexts {
		evictedIt := inMemoryJobRepo.GetJobIterator(qctx.Queue)
		if jobRepo == nil || reflect.ValueOf(jobRepo).IsNil() {
			jobIteratorByQueue[qctx.Queue] = evictedIt
		} else {
			queueIt := NewQueuedJobsIterator(ctx, qctx.Queue, sch.schedulingContext.Pool, sch.constraints.GetMaxQueueLookBack(), sch.schedulingContext, jobRepo)
			jobIteratorByQueue[qctx.Queue] = NewMultiJobsIterator(evictedIt, queueIt)
		}
	}

	sched, err := NewQueueScheduler(
		sch.schedulingContext,
		sch.constraints,
		sch.floatingResourceTypes,
		sch.nodeDb,
		jobIteratorByQueue,
		considerPriorityCLassPriority,
	)
	if err != nil {
		return nil, err
	}
	result, err := sched.Schedule(ctx)
	if err != nil {
		return nil, err
	}
	if len(result.PreemptedJobs) != 0 {
		return nil, errors.New("unexpected preemptions during scheduling")
	}
	if err := sch.updateGangAccounting(nil, result.ScheduledJobs); err != nil {
		return nil, err
	}
	return result, nil
}

// Unbind any preempted from the nodes they were evicted (and not re-scheduled) on.
func (sch *PreemptingQueueScheduler) unbindJobs(jctxs []*schedulercontext.JobSchedulingContext) error {
	for nodeId, jobsOnNode := range armadaslices.MapAndGroupByFuncs(
		jctxs,
		func(jctx *schedulercontext.JobSchedulingContext) string {
			return sch.nodeIdByJobId[jctx.JobId]
		},
		func(jcxt *schedulercontext.JobSchedulingContext) *jobdb.Job {
			return jcxt.Job
		},
	) {
		node, err := sch.nodeDb.GetNode(nodeId)
		if err != nil {
			return err
		}
		node, err = sch.nodeDb.UnbindJobsFromNode(jobsOnNode, node)
		if err != nil {
			return err
		}
		if err := sch.nodeDb.Upsert(node); err != nil {
			return err
		}
	}
	return nil
}

// Update sch.gangIdByJobId and sch.jobIdsByGangId based on preempted/scheduled jobs.
func (sch *PreemptingQueueScheduler) updateGangAccounting(preempted []*schedulercontext.JobSchedulingContext, scheduled []*schedulercontext.JobSchedulingContext) error {
	for _, jctx := range preempted {
		if gangId, ok := sch.gangIdByJobId[jctx.Job.Id()]; ok {
			delete(sch.gangIdByJobId, jctx.Job.Id())
			delete(sch.jobIdsByGangId, gangId)
		}
	}
	for _, jctx := range scheduled {
		if gangId := jctx.GangInfo.Id; gangId != "" {
			sch.gangIdByJobId[jctx.JobId] = gangId
			if m := sch.jobIdsByGangId[gangId]; m != nil {
				m[jctx.JobId] = true
			} else {
				sch.jobIdsByGangId[gangId] = map[string]bool{jctx.JobId: true}
			}
		}
	}
	return nil
}

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

type queueChecker interface {
	QueueContextExists(job *jobdb.Job) bool
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
