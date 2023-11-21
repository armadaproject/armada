package scheduler

import (
	"math/rand"
	"reflect"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/types"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// PreemptingQueueScheduler is a scheduler that makes a unified decisions on which jobs to preempt and schedule.
// Uses QueueScheduler as a building block.
type PreemptingQueueScheduler struct {
	schedulingContext                       *schedulercontext.SchedulingContext
	constraints                             schedulerconstraints.SchedulingConstraints
	nodeEvictionProbability                 float64
	nodeOversubscriptionEvictionProbability float64
	protectedFractionOfFairShare            float64
	jobRepo                                 JobRepository
	nodeDb                                  *nodedb.NodeDb
	// Maps job ids to the id of the node the job is associated with.
	// For scheduled or running jobs, that is the node the job is assigned to.
	// For preempted jobs, that is the node the job was preempted from.
	nodeIdByJobId map[string]string
	// Maps gang ids to the ids of jobs in that gang.
	jobIdsByGangId map[string]map[string]bool
	// Maps job ids of gang jobs to the id of that gang.
	gangIdByJobId map[string]string
	// If true, the unsuccessfulSchedulingKeys check of gangScheduler is omitted.
	skipUnsuccessfulSchedulingKeyCheck bool
	// If true, asserts that the nodeDb state is consistent with expected changes.
	enableAssertions bool
	// If true, a newer preemption strategy is used.
	enableNewPreemptionStrategy bool
}

func NewPreemptingQueueScheduler(
	sctx *schedulercontext.SchedulingContext,
	constraints schedulerconstraints.SchedulingConstraints,
	nodeEvictionProbability float64,
	nodeOversubscriptionEvictionProbability float64,
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
		schedulingContext:                       sctx,
		constraints:                             constraints,
		nodeEvictionProbability:                 nodeEvictionProbability,
		nodeOversubscriptionEvictionProbability: nodeOversubscriptionEvictionProbability,
		protectedFractionOfFairShare:            protectedFractionOfFairShare,
		jobRepo:                                 jobRepo,
		nodeDb:                                  nodeDb,
		nodeIdByJobId:                           maps.Clone(initialNodeIdByJobId),
		jobIdsByGangId:                          initialJobIdsByGangId,
		gangIdByJobId:                           maps.Clone(initialGangIdByJobId),
	}
}

func (sch *PreemptingQueueScheduler) EnableAssertions() {
	sch.enableAssertions = true
}

func (sch *PreemptingQueueScheduler) SkipUnsuccessfulSchedulingKeyCheck() {
	sch.skipUnsuccessfulSchedulingKeyCheck = true
}

func (sch *PreemptingQueueScheduler) EnableNewPreemptionStrategy() {
	sch.enableNewPreemptionStrategy = true
	sch.nodeDb.EnableNewPreemptionStrategy()
}

// Schedule
// - preempts jobs belonging to queues with total allocation above their fair share and
// - schedules new jobs belonging to queues with total allocation less than their fair share.
func (sch *PreemptingQueueScheduler) Schedule(ctx *armadacontext.Context) (*schedulerobjects.SchedulerResult, error) {
	defer func() {
		sch.schedulingContext.Finished = time.Now()
	}()

	preemptedJobsById := make(map[string]interfaces.LegacySchedulerJob)
	scheduledJobsById := make(map[string]interfaces.LegacySchedulerJob)

	// NodeDb snapshot prior to making any changes.
	// We compare against this snapshot after scheduling to detect changes.
	snapshot := sch.nodeDb.Txn(false)

	// Evict preemptible jobs.
	totalCost := sch.schedulingContext.TotalCost()
	evictorResult, inMemoryJobRepo, err := sch.evict(
		armadacontext.WithLogField(ctx, "stage", "evict for resource balancing"),
		NewNodeEvictor(
			sch.jobRepo,
			sch.schedulingContext.PriorityClasses,
			sch.nodeEvictionProbability,
			func(ctx *armadacontext.Context, job interfaces.LegacySchedulerJob) bool {
				if job.GetAnnotations() == nil {
					ctx.Errorf("can't evict job %s: annotations not initialised", job.GetId())
					return false
				}
				if job.GetNodeSelector() == nil {
					ctx.Errorf("can't evict job %s: nodeSelector not initialised", job.GetId())
					return false
				}
				if qctx, ok := sch.schedulingContext.QueueSchedulingContexts[job.GetQueue()]; ok {
					fairShare := qctx.Weight / sch.schedulingContext.WeightSum
					actualShare := sch.schedulingContext.FairnessCostProvider.CostFromQueue(qctx) / totalCost
					fractionOfFairShare := actualShare / fairShare
					if fractionOfFairShare <= sch.protectedFractionOfFairShare {
						return false
					}
				}
				if priorityClass, ok := sch.schedulingContext.PriorityClasses[job.GetPriorityClassName()]; ok {
					return priorityClass.Preemptible
				}
				return false
			},
			nil,
		),
	)
	if err != nil {
		return nil, err
	}
	for _, jctx := range evictorResult.EvictedJctxsByJobId {
		preemptedJobsById[jctx.Job.GetId()] = jctx.Job
	}
	maps.Copy(sch.nodeIdByJobId, evictorResult.NodeIdByJobId)

	// Re-schedule evicted jobs/schedule new jobs.
	schedulerResult, err := sch.schedule(
		armadacontext.WithLogField(ctx, "stage", "re-schedule after balancing eviction"),
		inMemoryJobRepo,
		sch.jobRepo,
	)
	if err != nil {
		return nil, err
	}
	for _, job := range schedulerResult.ScheduledJobs {
		if _, ok := preemptedJobsById[job.GetId()]; ok {
			delete(preemptedJobsById, job.GetId())
		} else {
			scheduledJobsById[job.GetId()] = job
		}
	}
	maps.Copy(sch.nodeIdByJobId, schedulerResult.NodeIdByJobId)

	// Evict jobs on oversubscribed nodes.
	evictorResult, inMemoryJobRepo, err = sch.evict(
		armadacontext.WithLogField(ctx, "stage", "evict oversubscribed"),
		NewOversubscribedEvictor(
			sch.jobRepo,
			sch.schedulingContext.PriorityClasses,
			sch.schedulingContext.DefaultPriorityClass,
			sch.nodeOversubscriptionEvictionProbability,
			nil,
		),
	)
	if err != nil {
		return nil, err
	}
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
			preemptedJobsById[jobId] = jctx.Job
		}
	}
	maps.Copy(sch.nodeIdByJobId, evictorResult.NodeIdByJobId)

	// Re-schedule evicted jobs/schedule new jobs.
	// Only necessary if a non-zero number of jobs were evicted.
	if len(evictorResult.EvictedJctxsByJobId) > 0 {
		// Since no new jobs are considered in this round, the scheduling key check brings no benefit.
		sch.SkipUnsuccessfulSchedulingKeyCheck()
		schedulerResult, err = sch.schedule(
			armadacontext.WithLogField(ctx, "stage", "schedule after oversubscribed eviction"),
			inMemoryJobRepo,
			// Only evicted jobs should be scheduled in this round.
			nil,
		)
		if err != nil {
			return nil, err
		}
		for _, job := range schedulerResult.ScheduledJobs {
			if _, ok := preemptedJobsById[job.GetId()]; ok {
				delete(preemptedJobsById, job.GetId())
			} else {
				scheduledJobsById[job.GetId()] = job
			}
			delete(scheduledAndEvictedJobsById, job.GetId())
		}
		maps.Copy(sch.nodeIdByJobId, schedulerResult.NodeIdByJobId)
	}

	preemptedJobs := maps.Values(preemptedJobsById)
	scheduledJobs := maps.Values(scheduledJobsById)
	if err := sch.unbindJobs(append(
		slices.Clone(preemptedJobs),
		maps.Values(scheduledAndEvictedJobsById)...),
	); err != nil {
		return nil, err
	}
	if s := JobsSummary(preemptedJobs); s != "" {
		ctx.Infof("preempting running jobs; %s", s)
	}
	if s := JobsSummary(scheduledJobs); s != "" {
		ctx.Infof("scheduling new jobs; %s", s)
	}
	// TODO: Show failed jobs.
	if sch.enableAssertions {
		err := sch.assertions(
			snapshot,
			preemptedJobsById,
			scheduledJobsById,
			sch.nodeIdByJobId,
		)
		if err != nil {
			return nil, err
		}
	}
	return &schedulerobjects.SchedulerResult{
		PreemptedJobs:      preemptedJobs,
		ScheduledJobs:      scheduledJobs,
		FailedJobs:         schedulerResult.FailedJobs,
		NodeIdByJobId:      sch.nodeIdByJobId,
		SchedulingContexts: []*schedulercontext.SchedulingContext{sch.schedulingContext},
	}, nil
}

func (sch *PreemptingQueueScheduler) evict(ctx *armadacontext.Context, evictor *Evictor) (*EvictorResult, *InMemoryJobRepository, error) {
	if evictor == nil {
		return &EvictorResult{}, NewInMemoryJobRepository(), nil
	}
	txn := sch.nodeDb.Txn(true)
	defer txn.Abort()

	// Evict using the provided evictor.
	it, err := nodedb.NewNodesIterator(txn)
	if err != nil {
		return nil, nil, err
	}
	result, err := evictor.Evict(ctx, it)
	if err != nil {
		return nil, nil, err
	}
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
		if _, err := sch.schedulingContext.EvictJob(jctx.Job); err != nil {
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
	inMemoryJobRepo := NewInMemoryJobRepository()
	inMemoryJobRepo.EnqueueMany(evictedJctxs)
	txn.Commit()

	if sch.enableNewPreemptionStrategy {
		if err := sch.nodeDb.Reset(); err != nil {
			return nil, nil, err
		}
		if err := addEvictedJobsToNodeDb(ctx, sch.schedulingContext, sch.nodeDb, inMemoryJobRepo); err != nil {
			return nil, nil, err
		}
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
		sch.schedulingContext.PriorityClasses,
		gangNodeIds,
		gangJobIds,
	)
	if evictor == nil {
		// No gangs to evict.
		return &EvictorResult{}, nil
	}
	it, err := nodedb.NewNodesIterator(txn)
	if err != nil {
		return nil, err
	}
	evictorResult, err := evictor.Evict(ctx, it)
	if err != nil {
		return nil, err
	}
	return evictorResult, nil
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
		gangId, ok := sch.gangIdByJobId[jctx.Job.GetId()]
		if !ok {
			// Not a gang job.
			continue
		}

		// Override cardinality and min cardinality with the number of evicted jobs in this gang.
		jctx.GangCardinality = len(sch.jobIdsByGangId[gangId])
		jctx.GangMinCardinality = jctx.GangCardinality
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
		if nodeId, ok := jctx.GetNodeSelector(schedulerconfig.NodeIdLabel); ok {
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
func addEvictedJobsToNodeDb(ctx *armadacontext.Context, sctx *schedulercontext.SchedulingContext, nodeDb *nodedb.NodeDb, inMemoryJobRepo *InMemoryJobRepository) error {
	gangItByQueue := make(map[string]*QueuedGangIterator)
	for _, qctx := range sctx.QueueSchedulingContexts {
		gangItByQueue[qctx.Queue] = NewQueuedGangIterator(
			sctx,
			inMemoryJobRepo.GetJobIterator(qctx.Queue),
			0,
			false,
		)
	}
	qr := NewMinimalQueueRepositoryFromSchedulingContext(sctx)
	candidateGangIterator, err := NewCandidateGangIterator(qr, sctx.FairnessCostProvider, gangItByQueue)
	if err != nil {
		return err
	}
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	i := 0
	for {
		if gctx, err := candidateGangIterator.Peek(); err != nil {
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

func (sch *PreemptingQueueScheduler) schedule(ctx *armadacontext.Context, inMemoryJobRepo *InMemoryJobRepository, jobRepo JobRepository) (*schedulerobjects.SchedulerResult, error) {
	jobIteratorByQueue := make(map[string]JobIterator)
	for _, qctx := range sch.schedulingContext.QueueSchedulingContexts {
		evictedIt := inMemoryJobRepo.GetJobIterator(qctx.Queue)
		if jobRepo == nil || reflect.ValueOf(jobRepo).IsNil() {
			jobIteratorByQueue[qctx.Queue] = evictedIt
		} else {
			queueIt, err := NewQueuedJobsIterator(ctx, qctx.Queue, jobRepo, sch.schedulingContext.PriorityClasses)
			if err != nil {
				return nil, err
			}
			jobIteratorByQueue[qctx.Queue] = NewMultiJobsIterator(evictedIt, queueIt)
		}
	}

	// Reset the scheduling keys cache after evicting jobs.
	sch.schedulingContext.ClearUnfeasibleSchedulingKeys()

	sched, err := NewQueueScheduler(
		sch.schedulingContext,
		sch.constraints,
		sch.nodeDb,
		jobIteratorByQueue,
	)
	if err != nil {
		return nil, err
	}
	if sch.skipUnsuccessfulSchedulingKeyCheck {
		sched.SkipUnsuccessfulSchedulingKeyCheck()
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
func (sch *PreemptingQueueScheduler) unbindJobs(jobs []interfaces.LegacySchedulerJob) error {
	for nodeId, jobsOnNode := range armadaslices.GroupByFunc(
		jobs,
		func(job interfaces.LegacySchedulerJob) string {
			return sch.nodeIdByJobId[job.GetId()]
		},
	) {
		node, err := sch.nodeDb.GetNode(nodeId)
		if err != nil {
			return err
		}
		node, err = nodedb.UnbindJobsFromNode(sch.schedulingContext.PriorityClasses, jobsOnNode, node)
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
func (sch *PreemptingQueueScheduler) updateGangAccounting(evictedJctxs []*schedulercontext.JobSchedulingContext, scheduledJobs []interfaces.LegacySchedulerJob) error {
	for _, jctx := range evictedJctxs {
		if gangId, ok := sch.gangIdByJobId[jctx.Job.GetId()]; ok {
			delete(sch.gangIdByJobId, jctx.Job.GetId())
			delete(sch.jobIdsByGangId, gangId)
		}
	}
	for _, job := range scheduledJobs {
		gangId, _, _, isGangJob, err := GangIdAndCardinalityFromLegacySchedulerJob(job)
		if err != nil {
			return err
		}
		if isGangJob {
			sch.gangIdByJobId[job.GetId()] = gangId
			if m := sch.jobIdsByGangId[gangId]; m != nil {
				m[job.GetId()] = true
			} else {
				sch.jobIdsByGangId[gangId] = map[string]bool{job.GetId(): true}
			}
		}
	}
	return nil
}

// For each node in the NodeDb, compare assigned jobs relative to the initial snapshot.
// Jobs no longer assigned to a node are preemtped.
// Jobs assigned to a node that weren't earlier are scheduled.
//
// Compare the nodedb.NodeJobDiff with expected preempted/scheduled jobs to ensure NodeDb is consistent.
// This is only to validate that nothing unexpected happened during scheduling.
func (sch *PreemptingQueueScheduler) assertions(
	snapshot *memdb.Txn,
	preemptedJobsById,
	scheduledJobsById map[string]interfaces.LegacySchedulerJob,
	nodeIdByJobId map[string]string,
) error {
	// Compare two snapshots of the nodeDb to find jobs that
	// were preempted/scheduled between creating the snapshots.
	preempted, scheduled, err := nodedb.NodeJobDiff(snapshot, sch.nodeDb.Txn(false))
	if err != nil {
		return err
	}

	// Assert that jobs we expect to be preempted/scheduled are marked as such in the nodeDb.
	for jobId := range preemptedJobsById {
		if _, ok := preempted[jobId]; !ok {
			return errors.Errorf("inconsistent NodeDb: expected job %s to be preempted in nodeDb", jobId)
		}
	}
	for jobId := range scheduledJobsById {
		if _, ok := scheduled[jobId]; !ok {
			return errors.Errorf("inconsistent NodeDb: expected job %s to be scheduled in nodeDb", jobId)
		}
	}

	// Assert that jobs marked as preempted (scheduled) in the nodeDb are expected to be preempted (scheduled),
	// and that jobs are preempted/scheduled on the nodes we expect them to.
	for jobId, node := range preempted {
		if expectedNodeId, ok := nodeIdByJobId[jobId]; ok {
			if expectedNodeId != node.Id {
				return errors.Errorf(
					"inconsistent NodeDb: expected job %s to be preempted from node %s, but got %s",
					jobId, expectedNodeId, node.Id,
				)
			}
		} else {
			return errors.Errorf(
				"inconsistent NodeDb: expected job %s to be mapped to node %s, but found none",
				jobId, node.Id,
			)
		}
		if _, ok := preemptedJobsById[jobId]; !ok {
			return errors.Errorf("inconsistent NodeDb: didn't expect job %s to be preempted (job marked as preempted in NodeDb)", jobId)
		}
	}
	for jobId, node := range scheduled {
		if expectedNodeId, ok := nodeIdByJobId[jobId]; ok {
			if expectedNodeId != node.Id {
				return errors.Errorf(
					"inconsistent NodeDb: expected job %s to be on node %s, but got %s",
					jobId, expectedNodeId, node.Id,
				)
			}
		} else {
			return errors.Errorf(
				"inconsistent NodeDb: expected job %s to be mapped to node %s, but found none",
				jobId, node.Id,
			)
		}
		if _, ok := scheduledJobsById[jobId]; !ok {
			return errors.Errorf("inconsistent NodeDb: didn't expect job %s to be scheduled (job marked as scheduled in NodeDb)", jobId)
		}
	}
	return nil
}

type Evictor struct {
	jobRepo         JobRepository
	priorityClasses map[string]types.PriorityClass
	nodeFilter      func(*armadacontext.Context, *nodedb.Node) bool
	jobFilter       func(*armadacontext.Context, interfaces.LegacySchedulerJob) bool
}

type EvictorResult struct {
	// For all evicted jobs, map from job id to the scheduling context for re-scheduling that job.
	EvictedJctxsByJobId map[string]*schedulercontext.JobSchedulingContext
	// Map from node id to node, containing all nodes on which at least one job was evicted.
	AffectedNodesById map[string]*nodedb.Node
	// For each evicted job, maps the id of the job to the id of the node it was evicted from.
	NodeIdByJobId map[string]string
}

func NewNodeEvictor(
	jobRepo JobRepository,
	priorityClasses map[string]types.PriorityClass,
	perNodeEvictionProbability float64,
	jobFilter func(*armadacontext.Context, interfaces.LegacySchedulerJob) bool,
	random *rand.Rand,
) *Evictor {
	if perNodeEvictionProbability <= 0 {
		return nil
	}
	if random == nil {
		random = rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	}
	return &Evictor{
		jobRepo:         jobRepo,
		priorityClasses: priorityClasses,
		nodeFilter: func(_ *armadacontext.Context, node *nodedb.Node) bool {
			return len(node.AllocatedByJobId) > 0 && random.Float64() < perNodeEvictionProbability
		},
		jobFilter: jobFilter,
	}
}

// NewFilteredEvictor returns a new evictor that evicts all jobs for which jobIdsToEvict[jobId] is true
// on nodes for which nodeIdsToEvict[nodeId] is true.
func NewFilteredEvictor(
	jobRepo JobRepository,
	priorityClasses map[string]types.PriorityClass,
	nodeIdsToEvict map[string]bool,
	jobIdsToEvict map[string]bool,
) *Evictor {
	if len(nodeIdsToEvict) == 0 || len(jobIdsToEvict) == 0 {
		return nil
	}
	return &Evictor{
		jobRepo:         jobRepo,
		priorityClasses: priorityClasses,
		nodeFilter: func(_ *armadacontext.Context, node *nodedb.Node) bool {
			shouldEvict := nodeIdsToEvict[node.Id]
			return shouldEvict
		},
		jobFilter: func(_ *armadacontext.Context, job interfaces.LegacySchedulerJob) bool {
			shouldEvict := jobIdsToEvict[job.GetId()]
			return shouldEvict
		},
	}
}

// NewOversubscribedEvictor returns a new evictor that
// for each node evicts all preemptible jobs of a priority class for which at least one job could not be scheduled
// with probability perNodeEvictionProbability.
func NewOversubscribedEvictor(
	jobRepo JobRepository,
	priorityClasses map[string]types.PriorityClass,
	defaultPriorityClass string,
	perNodeEvictionProbability float64,
	random *rand.Rand,
) *Evictor {
	if perNodeEvictionProbability <= 0 {
		return nil
	}
	if random == nil {
		random = rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	}
	// Populating overSubscribedPriorities relies on
	// - nodeFilter being called once before all calls to jobFilter and
	// - jobFilter being called for all jobs on that node before moving on to another node.
	var overSubscribedPriorities map[int32]bool
	return &Evictor{
		jobRepo:         jobRepo,
		priorityClasses: priorityClasses,
		nodeFilter: func(_ *armadacontext.Context, node *nodedb.Node) bool {
			overSubscribedPriorities = make(map[int32]bool)
			for p, rl := range node.AllocatableByPriority {
				if p < 0 {
					// Negative priorities correspond to already evicted jobs.
					continue
				}
				for _, q := range rl.Resources {
					if q.Cmp(resource.Quantity{}) == -1 {
						overSubscribedPriorities[p] = true
						break
					}
				}
			}
			return len(overSubscribedPriorities) > 0 && random.Float64() < perNodeEvictionProbability
		},
		jobFilter: func(ctx *armadacontext.Context, job interfaces.LegacySchedulerJob) bool {
			if job.GetAnnotations() == nil {
				ctx.Warnf("can't evict job %s: annotations not initialised", job.GetId())
				return false
			}
			priorityClassName := job.GetPriorityClassName()
			priorityClass, ok := priorityClasses[priorityClassName]
			if !ok {
				priorityClass = priorityClasses[defaultPriorityClass]
			}
			if priorityClass.Preemptible && overSubscribedPriorities[priorityClass.Priority] {
				return true
			}
			return false
		},
	}
}

// Evict removes jobs from nodes, returning all affected jobs and nodes.
// Any node for which nodeFilter returns false is skipped.
// Any job for which jobFilter returns true is evicted (if the node was not skipped).
// If a job was evicted from a node, postEvictFunc is called with the corresponding job and node.
func (evi *Evictor) Evict(ctx *armadacontext.Context, it nodedb.NodeIterator) (*EvictorResult, error) {
	var jobFilter func(job interfaces.LegacySchedulerJob) bool
	if evi.jobFilter != nil {
		jobFilter = func(job interfaces.LegacySchedulerJob) bool { return evi.jobFilter(ctx, job) }
	}
	evictedJctxsByJobId := make(map[string]*schedulercontext.JobSchedulingContext)
	affectedNodesById := make(map[string]*nodedb.Node)
	nodeIdByJobId := make(map[string]string)
	for node := it.NextNode(); node != nil; node = it.NextNode() {
		if evi.nodeFilter != nil && !evi.nodeFilter(ctx, node) {
			continue
		}
		jobIds := make([]string, 0, len(node.AllocatedByJobId))
		for jobId := range node.AllocatedByJobId {
			if _, ok := node.EvictedJobRunIds[jobId]; !ok {
				jobIds = append(jobIds, jobId)
			}
		}
		jobs, err := evi.jobRepo.GetExistingJobsByIds(jobIds)
		if err != nil {
			return nil, err
		}
		evictedJobs, node, err := nodedb.EvictJobsFromNode(evi.priorityClasses, jobFilter, jobs, node)
		if err != nil {
			return nil, err
		}

		// TODO: Should be safe to remove now.
		for i, evictedJob := range evictedJobs {
			if dbJob, ok := evictedJob.(*jobdb.Job); ok {
				evictedJobs[i] = dbJob.DeepCopy()
			}
		}

		for _, job := range evictedJobs {
			// Create a scheduling context for when re-scheduling this job.
			// Mark as evicted and add a node selector to ensure the job is re-scheduled onto the node it was evicted from.
			jctx := schedulercontext.JobSchedulingContextFromJob(evi.priorityClasses, job, GangIdAndCardinalityFromAnnotations)

			// TODO: This is only necessary for jobs not scheduled in this cycle.
			jctx.IsEvicted = true
			jctx.AddNodeSelector(schedulerconfig.NodeIdLabel, node.Id)
			evictedJctxsByJobId[job.GetId()] = jctx

			nodeIdByJobId[job.GetId()] = node.Id
		}
		if len(evictedJobs) > 0 {
			affectedNodesById[node.Id] = node
		}
	}
	result := &EvictorResult{
		EvictedJctxsByJobId: evictedJctxsByJobId,
		AffectedNodesById:   affectedNodesById,
		NodeIdByJobId:       nodeIdByJobId,
	}
	return result, nil
}
