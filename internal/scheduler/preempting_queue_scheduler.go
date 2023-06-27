package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
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

// Schedule
// - preempts jobs belonging to queues with total allocation above their fair share and
// - schedules new jobs belonging to queues with total allocation less than their fair share.
func (sch *PreemptingQueueScheduler) Schedule(ctx context.Context) (*SchedulerResult, error) {
	log := ctxlogrus.Extract(ctx)
	log = log.WithField("service", "PreemptingQueueScheduler")
	if sch.schedulingContext.TotalResources.AsWeightedMillis(sch.schedulingContext.ResourceScarcity) == 0 {
		// This refers to resources available across all clusters, i.e.,
		// it may include resources not currently considered for scheduling.
		log.Infof(
			"no resources with non-zero weight available for scheduling on any cluster: resource scarcity %v, total resources %v",
			sch.schedulingContext.ResourceScarcity, sch.schedulingContext.TotalResources,
		)
		return &SchedulerResult{}, nil
	}
	if rl := sch.nodeDb.TotalResources(); rl.AsWeightedMillis(sch.schedulingContext.ResourceScarcity) == 0 {
		// This refers to the resources currently considered for scheduling.
		log.Infof(
			"no resources with non-zero weight available for scheduling in NodeDb: resource scarcity %v, total resources %v",
			sch.schedulingContext.ResourceScarcity, sch.nodeDb.TotalResources(),
		)
		return &SchedulerResult{}, nil
	}
	defer func() {
		sch.schedulingContext.Finished = time.Now()
	}()

	preemptedJobsById := make(map[string]interfaces.LegacySchedulerJob)
	scheduledJobsById := make(map[string]interfaces.LegacySchedulerJob)
	log.Infof(
		"starting scheduling with total resources %s",
		sch.schedulingContext.TotalResources.CompactString(),
	)

	// NodeDb snapshot prior to making any changes.
	// We compare against this snapshot after scheduling to detect changes.
	snapshot := sch.nodeDb.Txn(false)

	// Evict preemptible jobs.
	totalCost, totalWeight := sch.schedulingContext.TotalCostAndWeight()
	evictorResult, inMemoryJobRepo, err := sch.evict(
		ctxlogrus.ToContext(
			ctx,
			log.WithField("stage", "evict for resource balancing"),
		),
		NewNodeEvictor(
			sch.jobRepo,
			sch.schedulingContext.PriorityClasses,
			sch.nodeEvictionProbability,
			func(ctx context.Context, job interfaces.LegacySchedulerJob) bool {
				if job.GetAnnotations() == nil {
					log := ctxlogrus.Extract(ctx)
					log.Errorf("can't evict job %s: annotations not initialised", job.GetId())
					return false
				}
				if job.GetNodeSelector() == nil {
					log := ctxlogrus.Extract(ctx)
					log.Errorf("can't evict job %s: nodeSelector not initialised", job.GetId())
					return false
				}
				if qctx, ok := sch.schedulingContext.QueueSchedulingContexts[job.GetQueue()]; ok {
					fairShare := qctx.Weight / totalWeight
					actualShare := qctx.TotalCostForQueue() / totalCost
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
	maps.Copy(preemptedJobsById, evictorResult.EvictedJobsById)
	maps.Copy(sch.nodeIdByJobId, evictorResult.NodeIdByJobId)

	// Re-schedule evicted jobs/schedule new jobs.
	schedulerResult, err := sch.schedule(
		ctxlogrus.ToContext(
			ctx,
			log.WithField("stage", "re-schedule after balancing eviction"),
		),
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
		ctxlogrus.ToContext(
			ctx,
			log.WithField("stage", "evict oversubscribed"),
		),
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
			_, ok := evictorResult.EvictedJobsById[jobId]
			return ok
		},
	)
	for jobId, job := range evictorResult.EvictedJobsById {
		if _, ok := scheduledJobsById[jobId]; ok {
			delete(scheduledJobsById, jobId)
		} else {
			preemptedJobsById[jobId] = job
		}
	}
	maps.Copy(sch.nodeIdByJobId, evictorResult.NodeIdByJobId)

	// Re-schedule evicted jobs/schedule new jobs.
	// Only necessary if a non-zero number of jobs were evicted.
	if len(evictorResult.EvictedJobsById) > 0 {
		// Since no new jobs are considered in this round, the scheduling key check brings no benefit.
		sch.SkipUnsuccessfulSchedulingKeyCheck()
		schedulerResult, err = sch.schedule(
			ctxlogrus.ToContext(
				ctx,
				log.WithField("stage", "schedule after oversubscribed eviction"),
			),
			inMemoryJobRepo,
			// Only evicted jobs should be scheduled in this round,
			// so we provide an empty repo for queued jobs.
			NewInMemoryJobRepository(sch.schedulingContext.PriorityClasses),
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
		log.Infof("preempting running jobs; %s", s)
	}
	if s := JobsSummary(scheduledJobs); s != "" {
		log.Infof("scheduling new jobs; %s", s)
	}
	if sch.enableAssertions {
		err := sch.assertions(
			ctxlogrus.ToContext(
				ctx,
				log.WithField("stage", "validate consistency"),
			),
			snapshot,
			preemptedJobsById,
			scheduledJobsById,
			sch.nodeIdByJobId,
		)
		if err != nil {
			return nil, err
		}
	}
	return &SchedulerResult{
		PreemptedJobs: preemptedJobs,
		ScheduledJobs: scheduledJobs,
		NodeIdByJobId: sch.nodeIdByJobId,
	}, nil
}

func (sch *PreemptingQueueScheduler) evict(ctx context.Context, evictor *Evictor) (*EvictorResult, *InMemoryJobRepository, error) {
	if evictor == nil {
		return &EvictorResult{}, NewInMemoryJobRepository(sch.schedulingContext.PriorityClasses), nil
	}
	log := ctxlogrus.Extract(ctx)
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
	maps.Copy(result.AffectedNodesById, gangEvictorResult.AffectedNodesById)
	maps.Copy(result.EvictedJobsById, gangEvictorResult.EvictedJobsById)
	maps.Copy(result.NodeIdByJobId, gangEvictorResult.NodeIdByJobId)

	if err := sch.setEvictedGangCardinality(result.EvictedJobsById); err != nil {
		return nil, nil, err
	}
	evictedJobs := maps.Values(result.EvictedJobsById)
	for _, job := range evictedJobs {
		if _, err := sch.schedulingContext.EvictJob(job); err != nil {
			return nil, nil, err
		}
	}
	// TODO: Move gang accounting into context.
	if err := sch.updateGangAccounting(evictedJobs, nil); err != nil {
		return nil, nil, err
	}
	if err := sch.evictionAssertions(result.EvictedJobsById, result.AffectedNodesById); err != nil {
		return nil, nil, err
	}
	if s := JobsSummary(evictedJobs); s != "" {
		log.Infof("evicted %d jobs on nodes %v; %s", len(evictedJobs), maps.Keys(result.AffectedNodesById), s)
	}
	inMemoryJobRepo := NewInMemoryJobRepository(sch.schedulingContext.PriorityClasses)
	inMemoryJobRepo.EnqueueMany(evictedJobs)
	txn.Commit()
	return result, inMemoryJobRepo, nil
}

// When evicting jobs, gangs may have been partially evicted.
// Here, we evict all jobs in any gang for which at least one job was already evicted.
func (sch *PreemptingQueueScheduler) evictGangs(ctx context.Context, txn *memdb.Txn, previousEvictorResult *EvictorResult) (*EvictorResult, error) {
	gangJobIds, gangNodeIds, err := sch.collectIdsForGangEviction(previousEvictorResult.EvictedJobsById)
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
func (sch *PreemptingQueueScheduler) collectIdsForGangEviction(evictedJobsById map[string]interfaces.LegacySchedulerJob) (map[string]bool, map[string]bool, error) {
	allGangJobIds := make(map[string]bool)
	gangNodeIds := make(map[string]bool)
	seenGangs := make(map[string]bool)
	for jobId := range evictedJobsById {
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
		gangJobIds := sch.jobIdsByGangId[gangId]
		if len(gangJobIds) == 0 {
			return nil, nil, errors.Errorf("no jobs found for gang %s", gangId)
		}
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
// Otherwise the evicted gang jobs will not be schedulable, since some gang jobs will be considered missing.
func (sch *PreemptingQueueScheduler) setEvictedGangCardinality(evictedJobsById map[string]interfaces.LegacySchedulerJob) error {
	for _, job := range evictedJobsById {
		gangId, ok := sch.gangIdByJobId[job.GetId()]
		if !ok {
			// Not a gang job.
			continue
		}
		gangCardinality := len(sch.jobIdsByGangId[gangId])
		annotations := job.GetAnnotations()
		if annotations == nil {
			return errors.Errorf("error setting gang cardinality for job %s: annotations not initialised", job.GetId())
		}
		annotations[configuration.GangCardinalityAnnotation] = fmt.Sprintf("%d", gangCardinality)
	}
	return nil
}

func (sch *PreemptingQueueScheduler) evictionAssertions(evictedJobsById map[string]interfaces.LegacySchedulerJob, affectedNodesById map[string]*schedulerobjects.Node) error {
	for _, qctx := range sch.schedulingContext.QueueSchedulingContexts {
		if !qctx.AllocatedByPriorityClass.IsStrictlyNonNegative() {
			return errors.Errorf("negative allocation for queue %s after eviction: %s", qctx.Queue, qctx.AllocatedByPriorityClass)
		}
	}
	evictedJobIdsByGangId := make(map[string]map[string]bool)
	for _, job := range evictedJobsById {
		jobId := job.GetId()
		if gangId, ok := sch.gangIdByJobId[jobId]; ok {
			if m := evictedJobIdsByGangId[gangId]; m != nil {
				m[jobId] = true
			} else {
				evictedJobIdsByGangId[gangId] = map[string]bool{jobId: true}
			}
		}
		if !isEvictedJob(job) {
			return errors.Errorf("evicted job %s is not marked as such: job annotations %v", jobId, job.GetAnnotations())
		}
		nodeSelector := job.GetNodeSelector()
		if nodeId, ok := targetNodeIdFromNodeSelector(nodeSelector); ok {
			if _, ok := affectedNodesById[nodeId]; !ok {
				return errors.Errorf("node id %s targeted by job %s is not marked as affected", nodeId, jobId)
			}
		} else {
			return errors.Errorf("evicted job %s is missing target node id selector: job nodeSelector %v", jobId, nodeSelector)
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

func (sch *PreemptingQueueScheduler) schedule(ctx context.Context, inMemoryJobRepo *InMemoryJobRepository, jobRepo JobRepository) (*SchedulerResult, error) {
	log := ctxlogrus.Extract(ctx)
	jobIteratorByQueue := make(map[string]JobIterator)
	for _, qctx := range sch.schedulingContext.QueueSchedulingContexts {
		evictedIt, err := inMemoryJobRepo.GetJobIterator(ctx, qctx.Queue)
		if err != nil {
			return nil, err
		}
		queueIt, err := NewQueuedJobsIterator(ctx, qctx.Queue, jobRepo)
		if err != nil {
			return nil, err
		}
		jobIteratorByQueue[qctx.Queue] = NewMultiJobsIterator(evictedIt, queueIt)
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
	if s := JobsSummary(result.ScheduledJobs); s != "" {
		log.Infof("re-scheduled %d jobs; %s", len(result.ScheduledJobs), s)
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
		node, err = nodedb.UnbindPodsFromNode(
			util.Map(
				jobsOnNode,
				func(job interfaces.LegacySchedulerJob) *schedulerobjects.PodRequirements {
					return PodRequirementFromLegacySchedulerJob(job, sch.schedulingContext.PriorityClasses)
				},
			),
			node,
		)
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
func (sch *PreemptingQueueScheduler) updateGangAccounting(preemptedJobs, scheduledJobs []interfaces.LegacySchedulerJob) error {
	for _, job := range preemptedJobs {
		if gangId, ok := sch.gangIdByJobId[job.GetId()]; ok {
			delete(sch.gangIdByJobId, job.GetId())
			delete(sch.jobIdsByGangId, gangId)
		}
	}
	for _, job := range scheduledJobs {
		gangId, _, isGangJob, err := GangIdAndCardinalityFromLegacySchedulerJob(job)
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
	ctx context.Context,
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
	priorityClasses map[string]configuration.PriorityClass
	nodeFilter      func(context.Context, *schedulerobjects.Node) bool
	jobFilter       func(context.Context, interfaces.LegacySchedulerJob) bool
	postEvictFunc   func(context.Context, interfaces.LegacySchedulerJob, *schedulerobjects.Node)
}

type EvictorResult struct {
	// Map from job id to job, containing all evicted jobs.
	EvictedJobsById map[string]interfaces.LegacySchedulerJob
	// Map from node id to node, containing all nodes on which at least one job was evicted.
	AffectedNodesById map[string]*schedulerobjects.Node
	// For each evicted job, maps the id of the job to the id of the node it was evicted from.
	NodeIdByJobId map[string]string
}

func NewNodeEvictor(
	jobRepo JobRepository,
	priorityClasses map[string]configuration.PriorityClass,
	perNodeEvictionProbability float64,
	jobFilter func(context.Context, interfaces.LegacySchedulerJob) bool,
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
		nodeFilter: func(_ context.Context, node *schedulerobjects.Node) bool {
			return len(node.AllocatedByJobId) > 0 && random.Float64() < perNodeEvictionProbability
		},
		jobFilter:     jobFilter,
		postEvictFunc: defaultPostEvictFunc,
	}
}

// NewFilteredEvictor returns a new evictor that evicts all jobs for which jobIdsToEvict[jobId] is true
// on nodes for which nodeIdsToEvict[nodeId] is true.
func NewFilteredEvictor(
	jobRepo JobRepository,
	priorityClasses map[string]configuration.PriorityClass,
	nodeIdsToEvict map[string]bool,
	jobIdsToEvict map[string]bool,
) *Evictor {
	if len(nodeIdsToEvict) == 0 || len(jobIdsToEvict) == 0 {
		return nil
	}
	return &Evictor{
		jobRepo:         jobRepo,
		priorityClasses: priorityClasses,
		nodeFilter: func(_ context.Context, node *schedulerobjects.Node) bool {
			shouldEvict := nodeIdsToEvict[node.Id]
			return shouldEvict
		},
		jobFilter: func(_ context.Context, job interfaces.LegacySchedulerJob) bool {
			shouldEvict := jobIdsToEvict[job.GetId()]
			return shouldEvict
		},
		postEvictFunc: defaultPostEvictFunc,
	}
}

// NewOversubscribedEvictor returns a new evictor that
// for each node evicts all preemptible jobs of a priority class for which at least one job could not be scheduled
// with probability perNodeEvictionProbability.
func NewOversubscribedEvictor(
	jobRepo JobRepository,
	priorityClasses map[string]configuration.PriorityClass,
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
		nodeFilter: func(_ context.Context, node *schedulerobjects.Node) bool {
			overSubscribedPriorities = make(map[int32]bool)
			for p, rl := range node.AllocatableByPriorityAndResource {
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
		jobFilter: func(ctx context.Context, job interfaces.LegacySchedulerJob) bool {
			if job.GetAnnotations() == nil {
				log := ctxlogrus.Extract(ctx)
				log.Warnf("can't evict job %s: annotations not initialised", job.GetId())
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
		postEvictFunc: defaultPostEvictFunc,
	}
}

// Evict removes jobs from nodes, returning all affected jobs and nodes.
// Any node for which nodeFilter returns false is skipped.
// Any job for which jobFilter returns true is evicted (if the node was not skipped).
// If a job was evicted from a node, postEvictFunc is called with the corresponding job and node.
func (evi *Evictor) Evict(ctx context.Context, it nodedb.NodeIterator) (*EvictorResult, error) {
	evictedJobsById := make(map[string]interfaces.LegacySchedulerJob)
	affectedNodesById := make(map[string]*schedulerobjects.Node)
	nodeIdByJobId := make(map[string]string)
	for node := it.NextNode(); node != nil; node = it.NextNode() {
		if evi.nodeFilter != nil && !evi.nodeFilter(ctx, node) {
			continue
		}
		jobIds := util.Filter(
			maps.Keys(node.AllocatedByJobId),
			func(jobId string) bool {
				_, ok := node.EvictedJobRunIds[jobId]
				return !ok
			},
		)
		jobs, err := evi.jobRepo.GetExistingJobsByIds(jobIds)
		if err != nil {
			return nil, err
		}
		for _, job := range jobs {
			if evi.jobFilter != nil && !evi.jobFilter(ctx, job) {
				continue
			}
			req := PodRequirementFromLegacySchedulerJob(job, evi.priorityClasses)
			if req == nil {
				continue
			}
			node, err = nodedb.EvictPodFromNode(req, node)
			if err != nil {
				return nil, err
			}
			if evi.postEvictFunc != nil {
				evi.postEvictFunc(ctx, job, node)
			}

			evictedJobsById[job.GetId()] = job
			nodeIdByJobId[job.GetId()] = node.Id
		}
		affectedNodesById[node.Id] = node
	}
	return &EvictorResult{
		EvictedJobsById:   evictedJobsById,
		AffectedNodesById: affectedNodesById,
		NodeIdByJobId:     nodeIdByJobId,
	}, nil
}

// TODO: This is only necessary for jobs not scheduled in this cycle.
// Since jobs scheduled in this cycle can be re-scheduled onto another node without triggering a preemption.
func defaultPostEvictFunc(ctx context.Context, job interfaces.LegacySchedulerJob, node *schedulerobjects.Node) {
	// Add annotation indicating to the scheduler this this job was evicted.
	annotations := job.GetAnnotations()
	if annotations == nil {
		log := ctxlogrus.Extract(ctx)
		log.Errorf("error evicting job %s: annotations not initialised", job.GetId())
	} else {
		annotations[schedulerconfig.IsEvictedAnnotation] = "true"
	}

	// Add node selector ensuring this job is only re-scheduled onto the node it was evicted from.
	nodeSelector := job.GetNodeSelector()
	if nodeSelector == nil {
		log := ctxlogrus.Extract(ctx)
		log.Errorf("error evicting job %s: nodeSelector not initialised", job.GetId())
	} else {
		nodeSelector[schedulerconfig.NodeIdLabel] = node.Id
	}
}
