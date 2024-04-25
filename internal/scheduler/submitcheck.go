package scheduler

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type schedulingResult struct {
	isSchedulable bool
	reason        string
}

type executorState struct {
	nodeDb                    *nodedb.NodeDb
	jobSchedulingResultsCache *lru.Cache
}

type SubmitScheduleChecker interface {
	Check(jobs []*jobdb.Job) (map[string]schedulingResult, error)
}

type SubmitChecker struct {
	schedulingConfig       configuration.SchedulingConfig
	executorRepository     database.ExecutorRepository
	schedulingKeyGenerator *schedulerobjects.SchedulingKeyGenerator
	state                  atomic.Pointer[executorState]
	clock                  clock.Clock // can  be  overridden for testing
}

func NewSubmitChecker(
	schedulingConfig configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
) *SubmitChecker {
	return &SubmitChecker{
		schedulingConfig:       schedulingConfig,
		executorRepository:     executorRepository,
		schedulingKeyGenerator: schedulerobjects.NewSchedulingKeyGenerator(),
		clock:                  clock.RealClock{},
	}
}

func (srv *SubmitChecker) Run(ctx *armadacontext.Context) error {
	srv.updateExecutors(ctx)
	ticker := time.NewTicker(srv.schedulingConfig.ExecutorUpdateFrequency)
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
	jobSchedulingResultsCache, err := lru.New(10000)
	if err != nil {
		// This should never happen as lru.New only returns errors
		// if it is initialised with an incorrect size
		panic(err)
	}
	nodeDb, err := srv.constructNodeDb(executors)
	if err == nil {
		newState := executorState{
			nodeDb:                    nodeDb,
			jobSchedulingResultsCache: jobSchedulingResultsCache,
		}
		srv.state.Store(&newState)
	} else {
		logging.
			WithStacktrace(ctx, err).
			Warnf("Error constructing nodedb")
	}
}

func (srv *SubmitChecker) Check(jobs []*jobdb.Job) (map[string]schedulingResult, error) {

	state := srv.state.Load()
	if state == nil {
		return nil, fmt.Errorf("executor state not loaded")
	}

	jobContexts := schedulercontext.JobSchedulingContextsFromJobs(srv.schedulingConfig.PriorityClasses, jobs)
	results := make(map[string]schedulingResult, len(jobs))

	// First, check if all jobs can be scheduled individually.
	for _, jctx := range jobContexts {
		results[jctx.JobId] = srv.getIndividualSchedulingResult(jctx, state)
	}

	// Then, check if all gangs can be scheduled.
	for gangId, jctxs := range armadaslices.GroupByFunc(
		jobContexts,
		func(jctx *schedulercontext.JobSchedulingContext) string {
			return jctx.GangInfo.Id
		},
	) {
		if gangId == "" {
			continue
		}
		gctx := schedulercontext.NewGangSchedulingContext(jctxs)
		if result := srv.getSchedulingResult(gctx, state); !result.isSchedulable {
			for _, jctx := range gctx.JobSchedulingContexts {
				results[jctx.JobId] = result
			}
		}
	}
	return results, nil
}

func (srv *SubmitChecker) getIndividualSchedulingResult(jctx *schedulercontext.JobSchedulingContext, state *executorState) schedulingResult {

	schedulingKey, ok := jctx.Job.GetSchedulingKey()
	if !ok {
		schedulingKey = interfaces.SchedulingKeyFromLegacySchedulerJob(srv.schedulingKeyGenerator, jctx.Job)
	}

	if obj, ok := state.jobSchedulingResultsCache.Get(schedulingKey); ok {
		return obj.(schedulingResult)
	}

	gangInfo := jctx.GangInfo
	// Mark this job context as "not in a gang" for the individual scheduling check.
	jctx.GangInfo = schedulercontext.EmptyGangInfo(jctx.Job)
	defer func() {
		jctx.GangInfo = gangInfo
	}()

	gctx := schedulercontext.NewGangSchedulingContext([]*schedulercontext.JobSchedulingContext{jctx})
	result := srv.getSchedulingResult(gctx, state)

	state.jobSchedulingResultsCache.Add(schedulingKey, result)

	return result
}

// Check if a set of jobs can be scheduled onto some cluster.
func (srv *SubmitChecker) getSchedulingResult(gctx *schedulercontext.GangSchedulingContext, state *executorState) schedulingResult {
	// Skip submit checks if this batch contains less than the min cardinality jobs.
	// Reason:
	//  - We need to support submitting gang jobs across batches and allow for gang jobs to queue until min cardinality is satisfied.
	//  - We cannot verify if min cardinality jobs are schedulable unless we are given at least that many in a single batch.
	//  - A side effect of this is that users can submit jobs in gangs that skip this check and are never schedulable, which will be handled via queue-ttl.
	if len(gctx.JobSchedulingContexts) < gctx.GangInfo.MinimumCardinality {
		return schedulingResult{isSchedulable: true, reason: ""}
	}

	txn := state.nodeDb.Txn(true)
	ok, err := state.nodeDb.ScheduleManyWithTxn(txn, gctx)
	txn.Abort()

	if err != nil {
		return schedulingResult{
			false,
			err.Error(),
		}
	}

	if ok {
		return schedulingResult{isSchedulable: true}
	}

	numSuccessfullyScheduled := 0
	for _, jctx := range gctx.JobSchedulingContexts {
		if jctx.PodSchedulingContext.IsSuccessful() {
			numSuccessfullyScheduled++
		}
	}
	var sb strings.Builder
	if len(gctx.JobSchedulingContexts) == 1 {
		sb.WriteString(":\n")
		pctx := gctx.JobSchedulingContexts[0].PodSchedulingContext
		if pctx == nil {
			return schedulingResult{
				isSchedulable: false,
				reason:        fmt.Sprintf("No PodSchedulingContext could be resolved"),
			}
		}
		sb.WriteString(pctx.String())
		sb.WriteString("\n")
		sb.WriteString("---")
		sb.WriteString("\n")
	} else {
		sb.WriteString(
			fmt.Sprintf(
				": %d out of %d pods schedulable (minCardinality %d)\n",
				numSuccessfullyScheduled, len(gctx.JobSchedulingContexts), gctx.GangInfo.MinimumCardinality,
			),
		)
	}

	return schedulingResult{isSchedulable: false, reason: sb.String()}

}

func (srv *SubmitChecker) constructNodeDb(executors []*schedulerobjects.Executor) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		srv.schedulingConfig.PriorityClasses,
		0,
		srv.schedulingConfig.IndexedResources,
		srv.schedulingConfig.IndexedTaints,
		srv.schedulingConfig.IndexedNodeLabels,
		srv.schedulingConfig.WellKnownNodeTypes,
		stringinterner.New(10000),
	)
	if err != nil {
		return nil, err
	}
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node); err != nil {
				return nil, err
			}
		}
	}
	txn.Commit()
	err = nodeDb.ClearAllocated()
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}
