package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/types"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

type minimalExecutor struct {
	nodeDb     *nodedb.NodeDb
	updateTime time.Time
}

type schedulingResult struct {
	isSchedulable bool
	reason        string
}

const maxJobSchedulingResults = 10000

type SubmitScheduleChecker interface {
	CheckApiJobs(jobs []*api.Job) (bool, string)
	CheckJobDbJobs(jobs []*jobdb.Job) (bool, string)
}

type SubmitChecker struct {
	executorTimeout           time.Duration
	priorityClasses           map[string]types.PriorityClass
	gangIdAnnotation          string
	executorById              map[string]minimalExecutor
	priorities                []int32
	indexedResources          []configuration.IndexedResource
	indexedTaints             []string
	indexedNodeLabels         []string
	executorRepository        database.ExecutorRepository
	clock                     clock.Clock
	mu                        sync.Mutex
	schedulingKeyGenerator    *schedulerobjects.SchedulingKeyGenerator
	jobSchedulingResultsCache *lru.Cache
	ExecutorUpdateFrequency   time.Duration
}

func NewSubmitChecker(
	executorTimeout time.Duration,
	schedulingConfig configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
) *SubmitChecker {
	jobSchedulingResultsCache, err := lru.New(maxJobSchedulingResults)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return &SubmitChecker{
		executorTimeout:           executorTimeout,
		priorityClasses:           schedulingConfig.Preemption.PriorityClasses,
		gangIdAnnotation:          configuration.GangIdAnnotation,
		executorById:              map[string]minimalExecutor{},
		priorities:                schedulingConfig.Preemption.AllowedPriorities(),
		indexedResources:          schedulingConfig.IndexedResources,
		indexedTaints:             schedulingConfig.IndexedTaints,
		indexedNodeLabels:         schedulingConfig.IndexedNodeLabels,
		executorRepository:        executorRepository,
		clock:                     clock.RealClock{},
		schedulingKeyGenerator:    schedulerobjects.NewSchedulingKeyGenerator(),
		jobSchedulingResultsCache: jobSchedulingResultsCache,
		ExecutorUpdateFrequency:   schedulingConfig.ExecutorUpdateFrequency,
	}
}

func (srv *SubmitChecker) Run(ctx *armadacontext.Context) error {
	srv.updateExecutors(ctx)

	ticker := time.NewTicker(srv.ExecutorUpdateFrequency)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			srv.updateExecutors(ctx)
		}
	}
}

func (srv *SubmitChecker) updateExecutors(ctx *armadacontext.Context) {
	executors, err := srv.executorRepository.GetExecutors(ctx)
	if err != nil {
		logging.
			WithStacktrace(ctx, err).
			Error("Error fetching executors")
		return
	}
	for _, executor := range executors {
		nodeDb, err := srv.constructNodeDb(executor.Nodes)
		if err == nil {
			srv.mu.Lock()
			srv.executorById[executor.Id] = minimalExecutor{
				nodeDb:     nodeDb,
				updateTime: executor.LastUpdateTime,
			}
			srv.mu.Unlock()
			if err != nil {
				logging.
					WithStacktrace(ctx, err).
					Errorf("Error constructing node db for executor %s", executor.Id)
			}
		} else {
			logging.
				WithStacktrace(ctx, err).
				Warnf("Error clearing nodedb for executor %s", executor.Id)
		}
	}

	// Reset cache as the executors may have updated, changing what can be scheduled.
	// Create a new schedulingKeyGenerator to get a new initial state.
	srv.schedulingKeyGenerator = schedulerobjects.NewSchedulingKeyGenerator()
	srv.jobSchedulingResultsCache.Purge()
}

func (srv *SubmitChecker) CheckApiJobs(jobs []*api.Job) (bool, string) {
	return srv.check(schedulercontext.JobSchedulingContextsFromJobs(srv.priorityClasses, jobs, GangIdAndCardinalityFromAnnotations))
}

func (srv *SubmitChecker) CheckJobDbJobs(jobs []*jobdb.Job) (bool, string) {
	return srv.check(schedulercontext.JobSchedulingContextsFromJobs(srv.priorityClasses, jobs, GangIdAndCardinalityFromAnnotations))
}

func (srv *SubmitChecker) check(jctxs []*schedulercontext.JobSchedulingContext) (bool, string) {
	// First, check if all jobs can be scheduled individually.
	for i, jctx := range jctxs {
		// Override min cardinality to enable individual job scheduling checks, but reset after
		originalGangMinCardinality := jctx.GangMinCardinality
		jctx.GangMinCardinality = 1
		schedulingResult := srv.getIndividualSchedulingResult(jctx)
		jctx.GangMinCardinality = originalGangMinCardinality
		if !schedulingResult.isSchedulable {
			return schedulingResult.isSchedulable, fmt.Sprintf("%d-th job unschedulable:\n%s", i, schedulingResult.reason)
		}
	}
	// Then, check if all gangs can be scheduled.
	for gangId, jctxsInGang := range armadaslices.GroupByFunc(
		jctxs,
		func(jctx *schedulercontext.JobSchedulingContext) string {
			return jctx.Job.GetAnnotations()[srv.gangIdAnnotation]
		},
	) {
		if gangId == "" {
			continue
		}
		if schedulingResult := srv.getSchedulingResult(jctxsInGang); !schedulingResult.isSchedulable {
			return schedulingResult.isSchedulable, fmt.Sprintf("gang %s is unschedulable:\n%s", gangId, schedulingResult.reason)
		}
	}
	return true, ""
}

func (srv *SubmitChecker) getIndividualSchedulingResult(jctx *schedulercontext.JobSchedulingContext) schedulingResult {
	req := jctx.PodRequirements
	srv.mu.Lock()
	schedulingKey := srv.schedulingKeyGenerator.Key(
		req.NodeSelector,
		req.Affinity,
		req.Tolerations,
		req.ResourceRequirements.Requests,
		req.Priority,
	)
	srv.mu.Unlock()
	var result schedulingResult
	if obj, ok := srv.jobSchedulingResultsCache.Get(schedulingKey); ok {
		result = obj.(schedulingResult)
	} else {
		result = srv.getSchedulingResult([]*schedulercontext.JobSchedulingContext{jctx})
		srv.jobSchedulingResultsCache.Add(schedulingKey, result)
	}
	if !result.isSchedulable {
		return result
	}
	return schedulingResult{isSchedulable: true}
}

// Check if a set of jobs can be scheduled onto some cluster.
func (srv *SubmitChecker) getSchedulingResult(jctxs []*schedulercontext.JobSchedulingContext) schedulingResult {
	if len(jctxs) == 0 {
		return schedulingResult{isSchedulable: true, reason: ""}
	}

	// Make a shallow copy to avoid holding the lock and
	// preventing updating NodeDbs while checking if jobs can be scheduled
	srv.mu.Lock()
	executorById := maps.Clone(srv.executorById)
	srv.mu.Unlock()
	executorById = srv.filterStaleExecutors(executorById)
	if len(executorById) == 0 {
		return schedulingResult{isSchedulable: false, reason: "no executor clusters available"}
	}

	isSchedulable := false
	var sb strings.Builder
	for id, executor := range executorById {
		nodeDb := executor.nodeDb
		txn := nodeDb.Txn(true)
		// TODO: This doesn't account for per-queue limits or the NodeUniformityLabel.
		// We should create a GangScheduler for this instead.
		ok, err := nodeDb.ScheduleManyWithTxn(txn, jctxs)
		txn.Abort()

		isSchedulable = isSchedulable || ok

		sb.WriteString(id)
		if err != nil {
			sb.WriteString(err.Error())
			sb.WriteString("\n")
			continue
		}

		numSuccessfullyScheduled := 0
		for _, jctx := range jctxs {
			pctx := jctx.PodSchedulingContext
			if pctx != nil && pctx.NodeId != "" {
				numSuccessfullyScheduled++
			}
		}

		if len(jctxs) == 1 {
			sb.WriteString(":\n")
			for _, jctx := range jctxs {
				pctx := jctx.PodSchedulingContext
				if pctx == nil {
					continue
				}
				sb.WriteString(pctx.String())
				sb.WriteString("\n")
			}
			sb.WriteString("---")
			sb.WriteString("\n")
		} else {
			sb.WriteString(":")
			sb.WriteString(fmt.Sprintf(" %d out of %d pods schedulable (minCardinality %d)\n", numSuccessfullyScheduled, len(jctxs), jctxs[0].GangMinCardinality))
		}
	}
	return schedulingResult{isSchedulable: isSchedulable, reason: sb.String()}
}

func (srv *SubmitChecker) filterStaleExecutors(executorsById map[string]minimalExecutor) map[string]minimalExecutor {
	rv := make(map[string]minimalExecutor)
	for id, executor := range executorsById {
		if srv.clock.Since(executor.updateTime) < srv.executorTimeout {
			rv[id] = executor
		}
	}
	return rv
}

func (srv *SubmitChecker) constructNodeDb(nodes []*schedulerobjects.Node) (*nodedb.NodeDb, error) {
	// Nodes to be considered by the scheduler.
	// We just need to know if scheduling is possible;
	// no need to try to find a good fit.
	var maxExtraNodesToConsider uint = 0
	nodeDb, err := nodedb.NewNodeDb(
		srv.priorityClasses,
		maxExtraNodesToConsider,
		srv.indexedResources,
		srv.indexedTaints,
		srv.indexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	for _, node := range nodes {
		if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node); err != nil {
			return nil, err
		}
	}
	txn.Commit()
	if err != nil {
		return nil, err
	}
	err = nodeDb.ClearAllocated()
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}
