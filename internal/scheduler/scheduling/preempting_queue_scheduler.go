package scheduling

import (
	"context"
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
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/optimiser"
)

// PreemptingQueueScheduler is a scheduler that makes a unified decisions on which jobs to preempt and schedule.
// Uses QueueScheduler as a building block.
type PreemptingQueueScheduler struct {
	schedulingContext                *schedulercontext.SchedulingContext
	constraints                      schedulerconstraints.SchedulingConstraints
	floatingResourceTypes            *floatingresources.FloatingResourceTypes
	protectedFractionOfFairShare     float64
	maxQueueLookBack                 uint
	preferLargeJobOrdering           bool
	protectUncappedAdjustedFairShare bool
	jobRepo                          jobdb.JobRepository
	nodeDb                           *nodedb.NodeDb
	optimiserConfig                  *configuration.OptimiserConfig
	optimiserEnabled                 bool
	// Maps job ids to the id of the node the job is associated with.
	// For scheduled or running jobs, that is the node the job is assigned to.
	// For preempted jobs, that is the node the job was preempted from.
	nodeIdByJobId map[string]string
	// Maps gang ids to the ids of jobs in that gang.
	jobIdsByGangId map[string]map[string]bool
	// Maps job ids of gang jobs to the id of that gang.
	gangIdByJobId map[string]string
	marketDriven  bool
}

func NewPreemptingQueueScheduler(
	sctx *schedulercontext.SchedulingContext,
	constraints schedulerconstraints.SchedulingConstraints,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
	config configuration.SchedulingConfig,
	jobRepo jobdb.JobRepository,
	nodeDb *nodedb.NodeDb,
	initialNodeIdByJobId map[string]string,
	initialJobIdsByGangId map[string]map[string]bool,
	initialGangIdByJobId map[string]string,
	marketDriven bool,
	optimiserEnabled bool,
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
		schedulingContext:                sctx,
		constraints:                      constraints,
		floatingResourceTypes:            floatingResourceTypes,
		protectedFractionOfFairShare:     config.GetProtectedFractionOfFairShare(sctx.Pool),
		preferLargeJobOrdering:           config.EnablePreferLargeJobOrdering,
		protectUncappedAdjustedFairShare: config.GetProtectUncappedAdjustedFairShare(sctx.Pool),
		maxQueueLookBack:                 config.MaxQueueLookback,
		jobRepo:                          jobRepo,
		nodeDb:                           nodeDb,
		nodeIdByJobId:                    maps.Clone(initialNodeIdByJobId),
		jobIdsByGangId:                   initialJobIdsByGangId,
		gangIdByJobId:                    maps.Clone(initialGangIdByJobId),
		marketDriven:                     marketDriven,
		optimiserConfig:                  config.GetOptimiserConfig(sctx.Pool),
		optimiserEnabled:                 optimiserEnabled,
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
	ctx.Logger().WithField("stage", "scheduling-algo").Infof("Evicting preemptible jobs")
	evictorResult, inMemoryJobRepo, err := sch.evict(
		armadacontext.WithLogField(ctx, "stage", "evict for resource balancing"),
		NewNodeEvictor(
			sch.jobRepo,
			sch.nodeDb,
			func(ctx *armadacontext.Context, job *jobdb.Job) (bool, string) {
				if job.LatestRun().Pool() != sch.schedulingContext.Pool {
					return false, "" // Cross-pool home-away job. These are usually preempted via urgency-based preemption, so don't fill in cantPreemptReason.
				}
				if !sch.schedulingContext.QueueContextExists(job) {
					ctx.Warnf("No queue context found for job %s.  This job cannot be evicted", job.Id())
					return false, "invalid_queue"
				}
				if !job.PriorityClass().Preemptible {
					return false, "job_not_preemptible"
				}
				if job.Annotations() == nil {
					ctx.Errorf("can't evict job %s: annotations not initialised", job.Id())
					return false, "missing_annotations"
				}
				if job.NodeSelector() == nil {
					ctx.Errorf("can't evict job %s: nodeSelector not initialised", job.Id())
					return false, "missing_node_selector"
				}

				// If we are in market mode then everything is evictable
				if sch.marketDriven {
					return true, ""
				}

				if qctx, ok := sch.schedulingContext.QueueSchedulingContexts[job.Queue()]; ok {
					actualShare := sch.schedulingContext.FairnessCostProvider.UnweightedCostFromQueue(qctx)
					fairShare := math.Max(qctx.DemandCappedAdjustedFairShare, qctx.FairShare)
					if sch.protectUncappedAdjustedFairShare {
						fairShare = qctx.UncappedAdjustedFairShare
					}
					fractionOfFairShare := actualShare / fairShare
					if fractionOfFairShare <= sch.protectedFractionOfFairShare {
						return false, "below_protected_fair_share"
					}
				}
				return true, ""
			},
		),
	)
	if err != nil {
		return nil, err
	}
	ctx.Logger().WithField("stage", "scheduling-algo").Info("Finished evicting preemptible jobs")
	for _, jctx := range evictorResult.EvictedJctxsByJobId {
		preemptedJobsById[jctx.JobId] = jctx
	}
	maps.Copy(sch.nodeIdByJobId, evictorResult.NodeIdByJobId)

	// Re-schedule evicted jobs/schedule new jobs.
	ctx.Logger().WithField("stage", "scheduling-algo").Info("Performing initial scheduling of jobs onto nodes")
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
	ctx.Logger().WithField("stage", "scheduling-algo").Info("Finished initial scheduling of jobs onto nodes")
	for _, jctx := range schedulerResult.ScheduledJobs {
		if _, ok := preemptedJobsById[jctx.JobId]; ok {
			delete(preemptedJobsById, jctx.JobId)
		} else {
			scheduledJobsById[jctx.JobId] = jctx
		}
	}
	maps.Copy(sch.nodeIdByJobId, schedulerResult.NodeIdByJobId)

	// Evict jobs on oversubscribed nodes.
	ctx.Logger().WithField("stage", "scheduling-algo").Info("Evicting jobs from oversubscribed nodes")
	reevictResult, inMemoryJobRepo, err := sch.evict(
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
	ctx.Logger().WithField("stage", "scheduling-algo").Info("Finished evicting jobs from oversubscribed nodes")
	scheduledAndEvictedJobsById := armadamaps.FilterKeys(
		scheduledJobsById,
		func(jobId string) bool {
			_, ok := reevictResult.EvictedJctxsByJobId[jobId]
			return ok
		},
	)
	for jobId, jctx := range reevictResult.EvictedJctxsByJobId {
		if _, ok := scheduledJobsById[jobId]; ok {
			delete(scheduledJobsById, jobId)
		} else {
			preemptedJobsById[jobId] = jctx
		}
	}
	maps.Copy(sch.nodeIdByJobId, reevictResult.NodeIdByJobId)

	// Re-schedule evicted jobs/schedule new jobs.
	// Only necessary if a non-zero number of jobs were evicted.
	if len(reevictResult.EvictedJctxsByJobId) > 0 {
		ctx.Logger().WithField("stage", "scheduling-algo").Info("Performing second scheduling ")
		rescheduleSchedulerResult, rescheduleErr := sch.schedule(
			armadacontext.WithLogField(ctx, "stage", "schedule after oversubscribed eviction"),
			inMemoryJobRepo,
			// Only evicted jobs should be scheduled in this round.
			nil,
			true, // Since no new jobs are considered in this round, the scheduling key check brings no benefit.
			true, // when choosing which queue to consider use the priority class of the next job
		)
		if rescheduleErr != nil {
			return nil, rescheduleErr
		}
		ctx.Logger().WithField("stage", "scheduling-algo").Info("Finished second scheduling pass")
		for _, jctx := range rescheduleSchedulerResult.ScheduledJobs {
			if _, ok := preemptedJobsById[jctx.JobId]; ok {
				delete(preemptedJobsById, jctx.JobId)
			} else {
				scheduledJobsById[jctx.JobId] = jctx
			}
			delete(scheduledAndEvictedJobsById, jctx.JobId)
		}
		maps.Copy(sch.nodeIdByJobId, rescheduleSchedulerResult.NodeIdByJobId)
	}

	if sch.optimiserConfig != nil && sch.optimiserEnabled {
		optimisingSchedulerResult, err := sch.runOptimiser(ctx)
		if err != nil {
			return nil, err
		}
		for _, jctx := range optimisingSchedulerResult.ScheduledJobs {
			if _, ok := preemptedJobsById[jctx.JobId]; ok {
				// TODO Should this ever happen? We shouldn't ever be rescheduling evicted jobs here
				delete(preemptedJobsById, jctx.JobId)
			} else {
				scheduledJobsById[jctx.JobId] = jctx
			}
		}
		for _, jctx := range optimisingSchedulerResult.PreemptedJobs {
			if _, ok := scheduledJobsById[jctx.JobId]; ok {
				// Scheduled and preempted in same round, no need to actually preempt the job
				delete(scheduledJobsById, jctx.JobId)
			} else {
				preemptedJobsById[jctx.JobId] = jctx
			}
		}

		maps.Copy(sch.nodeIdByJobId, optimisingSchedulerResult.NodeIdByJobId)
	}

	preemptedJobs := maps.Values(preemptedJobsById)
	scheduledJobs := maps.Values(scheduledJobsById)
	ctx.Logger().WithField("stage", "scheduling-algo").Infof("Unbinding %d preempted and %d evicted jobs", len(preemptedJobs), len(maps.Values(scheduledAndEvictedJobsById)))
	if err := sch.unbindJobs(append(
		slices.Clone(preemptedJobs),
		maps.Values(scheduledAndEvictedJobsById)...),
	); err != nil {
		return nil, err
	}
	ctx.Logger().WithField("stage", "scheduling-algo").Infof("Finished unbinding preempted and evicted jobs")

	PopulatePreemptionDescriptions(preemptedJobs, scheduledJobs)
	schedulercontext.PrintJobSchedulingDetails(ctx, "Evicted job details", maps.Values(scheduledAndEvictedJobsById))
	schedulercontext.PrintJobSummary(ctx, "Preempting running jobs;", preemptedJobs)
	schedulercontext.PrintJobSummary(ctx, "Scheduling new jobs;", scheduledJobs)
	// TODO: Show failed jobs.

	schedulingStats := PerPoolSchedulingStats{
		StatsPerQueue:                schedulerResult.PerPoolSchedulingStats[sch.schedulingContext.Pool].StatsPerQueue,
		LoopNumber:                   schedulerResult.PerPoolSchedulingStats[sch.schedulingContext.Pool].LoopNumber,
		EvictorResult:                evictorResult,
		ProtectedFractionOfFairShare: sch.protectedFractionOfFairShare,
		NodeDb:                       sch.nodeDb,
	}

	return &SchedulerResult{
		PreemptedJobs:      preemptedJobs,
		ScheduledJobs:      scheduledJobs,
		NodeIdByJobId:      sch.nodeIdByJobId,
		SchedulingContexts: []*schedulercontext.SchedulingContext{sch.schedulingContext},
		PerPoolSchedulingStats: map[string]PerPoolSchedulingStats{
			sch.schedulingContext.Pool: schedulingStats,
		},
	}, nil
}

func (sch *PreemptingQueueScheduler) evict(ctx *armadacontext.Context, evictor *Evictor) (*EvictorResult, *InMemoryJobRepository, error) {
	if evictor == nil {
		return &EvictorResult{}, NewInMemoryJobRepository(sch.schedulingContext.Pool, jobdb.MarketSchedulingOrderCompare), nil
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
	ctx.Infof("Evicting remains of partially evicted gangs for pool %s (most may get re-scheduled this cycle so they won't necessarily be preempted) %s", sch.schedulingContext.Pool, gangEvictorResult.SummaryString())
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

	schedulingOrder := jobdb.SchedulingOrderCompare
	if sch.marketDriven {
		schedulingOrder = jobdb.MarketSchedulingOrderCompare
	}
	inMemoryJobRepo := NewInMemoryJobRepository(sch.schedulingContext.Pool, schedulingOrder)
	inMemoryJobRepo.EnqueueMany(evictedJctxs)
	txn.Commit()

	if err := sch.nodeDb.Reset(); err != nil {
		return nil, nil, err
	}
	if err := sch.addEvictedJobsToNodeDb(ctx, inMemoryJobRepo); err != nil {
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
	evictor := NewFilteredEvictor(
		sch.jobRepo,
		sch.nodeDb,
		gangNodeIds,
		gangJobIds,
		"gang_eviction_not_required",
	)
	if evictor == nil {
		// No gangs to evict.
		return &EvictorResult{}, nil
	}

	result, err := evictor.Evict(ctx, txn)
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
		if internaltypes.RlMapHasNegativeValues(qctx.AllocatedByPriorityClass) {
			return errors.Errorf("negative allocation for queue %s after eviction: %s",
				qctx.Queue,
				internaltypes.RlMapToString(qctx.AllocatedByPriorityClass),
			)
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
		queues[name] = MinimalQueue{allocation: qctx.Allocated, weight: qctx.Weight}
	}
	return &MinimalQueueRepository{queues: queues}
}

type MinimalQueue struct {
	allocation internaltypes.ResourceList
	weight     float64
}

func (q MinimalQueue) GetAllocation() internaltypes.ResourceList {
	return q.allocation
}

func (q MinimalQueue) GetWeight() float64 {
	return q.weight
}

// addEvictedJobsToNodeDb adds evicted jobs to the NodeDb.
// Needed to enable the nodeDb accounting for these when preempting.
func (sch *PreemptingQueueScheduler) addEvictedJobsToNodeDb(_ *armadacontext.Context, inMemoryJobRepo *InMemoryJobRepository) error {
	sctx := sch.schedulingContext
	gangItByQueue := make(map[string]*QueuedGangIterator)
	for _, qctx := range sch.schedulingContext.QueueSchedulingContexts {
		gangItByQueue[qctx.Queue] = NewQueuedGangIterator(
			sch.schedulingContext,
			inMemoryJobRepo.GetJobIterator(qctx.Queue),
			0,
			false,
		)
	}
	qr := NewMinimalQueueRepositoryFromSchedulingContext(sch.schedulingContext)
	var candidateGangIterator CandidateGangIterator
	var err error
	if sch.marketDriven {
		candidateGangIterator, err = NewMarketCandidateGangIterator(sctx.Pool, sctx, gangItByQueue)
		if err != nil {
			return err
		}
	} else {
		candidateGangIterator, err = NewCostBasedCandidateGangIterator(sctx.Pool, sctx, sctx.FairnessCostProvider, gangItByQueue, false, sch.preferLargeJobOrdering)
		if err != nil {
			return err
		}
	}
	txn := sch.nodeDb.Txn(true)
	defer txn.Abort()
	i := 0
	for {
		if gctx, _, err := candidateGangIterator.Peek(); err != nil {
			return err
		} else if gctx == nil {
			break
		} else {
			for _, jctx := range gctx.JobSchedulingContexts {
				if err := sch.nodeDb.AddEvictedJobSchedulingContextWithTxn(txn, i, jctx); err != nil {
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

func (sch *PreemptingQueueScheduler) runOptimiser(ctx *armadacontext.Context) (*SchedulerResult, error) {
	factory := sch.schedulingContext.TotalResources.Factory()
	var maximumJobSizeToPreempt *internaltypes.ResourceList
	if sch.optimiserConfig.MaximumJobSizeToPreempt != nil {
		maxJobSize := factory.FromJobResourceListIgnoreUnknown(*sch.optimiserConfig.MaximumJobSizeToPreempt)
		maximumJobSizeToPreempt = &maxJobSize
	}
	var minimumJobSizeToSchedule *internaltypes.ResourceList
	if sch.optimiserConfig.MinimumJobSizeToSchedule != nil {
		minJobSize := factory.FromJobResourceListIgnoreUnknown(*sch.optimiserConfig.MinimumJobSizeToSchedule)
		minimumJobSizeToSchedule = &minJobSize
	}

	nodeScheduler := optimiser.NewPreemptingNodeScheduler(sch.jobRepo, maximumJobSizeToPreempt)
	optimisingScheduler := optimiser.NewFairnessOptimisingScheduler(nodeScheduler, sch.jobRepo, sch.nodeDb, sch.optimiserConfig.MinimumFairnessImprovementPercentage)
	optimisingQueueScheduler := NewOptimisingQueueScheduler(
		sch.jobRepo,
		optimisingScheduler,
		sch.constraints,
		sch.floatingResourceTypes,
		sch.maxQueueLookBack,
		sch.preferLargeJobOrdering,
		sch.marketDriven,
		minimumJobSizeToSchedule,
		sch.optimiserConfig.MaximumJobsPerRound,
		sch.optimiserConfig.MaximumResourceFractionToSchedule)
	sch.schedulingContext.ClearUnfeasibleSchedulingKeys()

	timeoutContext, cancel := armadacontext.WithTimeout(ctx, sch.optimiserConfig.Timeout)
	defer cancel()

	result, err := optimisingQueueScheduler.Schedule(timeoutContext, sch.schedulingContext)
	if err != nil {
		// This is deliberately defensive to guard against the experimental optimiser causing the main scheduler issues
		if errors.Is(err, context.DeadlineExceeded) {
			ctx.Warnf("optimiser timed out, configured timeout %s", sch.optimiserConfig.Timeout)
			return &SchedulerResult{
				PreemptedJobs: []*schedulercontext.JobSchedulingContext{},
				ScheduledJobs: []*schedulercontext.JobSchedulingContext{},
				NodeIdByJobId: map[string]string{},
			}, nil
		}
		return nil, err
	}
	return result, nil
}

func (sch *PreemptingQueueScheduler) schedule(
	ctx *armadacontext.Context,
	inMemoryJobRepo *InMemoryJobRepository,
	jobRepo jobdb.JobRepository,
	skipUnsuccessfulSchedulingKeyCheck bool,
	considerPriorityCLassPriority bool,
) (*SchedulerResult, error) {
	sortOrder := jobdb.FairShareOrder
	if sch.marketDriven {
		sortOrder = jobdb.PriceOrder
	}
	jobIteratorByQueue := make(map[string]JobContextIterator)
	for _, qctx := range sch.schedulingContext.QueueSchedulingContexts {
		evictedIt := inMemoryJobRepo.GetJobIterator(qctx.Queue)
		if jobRepo == nil || reflect.ValueOf(jobRepo).IsNil() {
			jobIteratorByQueue[qctx.Queue] = evictedIt
		} else {
			queueIt := NewQueuedJobsIterator(ctx, qctx.Queue, sch.schedulingContext.Pool, jobRepo, sortOrder)
			if sch.marketDriven {
				jobIteratorByQueue[qctx.Queue] = NewMarketDrivenMultiJobsIterator(evictedIt, queueIt)
			} else {
				jobIteratorByQueue[qctx.Queue] = NewMultiJobsIterator(evictedIt, queueIt)
			}
		}
	}

	// Reset the scheduling keys cache after evicting jobs.
	sch.schedulingContext.ClearUnfeasibleSchedulingKeys()

	sched, err := NewQueueScheduler(
		sch.schedulingContext,
		sch.constraints,
		sch.floatingResourceTypes,
		sch.nodeDb,
		jobIteratorByQueue,
		skipUnsuccessfulSchedulingKeyCheck,
		considerPriorityCLassPriority,
		sch.preferLargeJobOrdering,
		sch.maxQueueLookBack,
		sch.marketDriven,
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
