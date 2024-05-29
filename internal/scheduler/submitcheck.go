package scheduler

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type schedulingResult struct {
	isSchedulable bool
	pools         []string
	reason        string
}

// TODO: rename this to "executor" when we simplify pool assigner
type executorDetails struct {
	pool           string
	nodeDb         *nodedb.NodeDb
	minimumJobSize schedulerobjects.ResourceList
}

type executorState struct {
	executorsById             map[string]*executorDetails
	jobSchedulingResultsCache *lru.Cache
}

type SubmitScheduleChecker interface {
	Check(ctx *armadacontext.Context, jobs []*jobdb.Job) (map[string]schedulingResult, error)
}

// DummySubmitChecker  is a  SubmitScheduleChecker that allows every job
type DummySubmitChecker struct{}

func (srv *DummySubmitChecker) Check(_ *armadacontext.Context, jobs []*jobdb.Job) (map[string]schedulingResult, error) {
	results := make(map[string]schedulingResult, len(jobs))
	for _, job := range jobs {
		results[job.Id()] = schedulingResult{isSchedulable: true}
	}
	return results, nil
}

type SubmitChecker struct {
	schedulingConfig    configuration.SchedulingConfig
	executorRepository  database.ExecutorRepository
	resourceListFactory *internaltypes.ResourceListFactory
	state               atomic.Pointer[executorState]
	clock               clock.Clock // can  be  overridden for testing
}

func NewSubmitChecker(
	schedulingConfig configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
	resourceListFactory *internaltypes.ResourceListFactory,
) *SubmitChecker {
	return &SubmitChecker{
		schedulingConfig:    schedulingConfig,
		executorRepository:  executorRepository,
		resourceListFactory: resourceListFactory,
		clock:               clock.RealClock{},
	}
}

func (srv *SubmitChecker) Run(ctx *armadacontext.Context) error {
	ctx.Infof("Will refresh executor state every %s", srv.schedulingConfig.ExecutorUpdateFrequency)
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
	ctx.Infof("Retrieved %d executors", len(executors))
	jobSchedulingResultsCache, err := lru.New(10000)
	if err != nil {
		// This should never happen as lru.New only returns an error if it is initialised with a negative size
		panic(err)
	}

	executorsById := map[string]*executorDetails{}
	for _, ex := range executors {
		nodeDb, err := srv.constructNodeDb(ex)
		if err == nil {
			executorsById[ex.Id] = &executorDetails{
				pool:           ex.Pool,
				nodeDb:         nodeDb,
				minimumJobSize: ex.MinimumJobSize,
			}
		} else {
			logging.
				WithStacktrace(ctx, err).
				Warnf("Error constructing nodedb for executor: %s", ex.Id)
		}
	}
	srv.state.Store(&executorState{
		executorsById:             executorsById,
		jobSchedulingResultsCache: jobSchedulingResultsCache,
	})
}

func (srv *SubmitChecker) Check(ctx *armadacontext.Context, jobs []*jobdb.Job) (map[string]schedulingResult, error) {
	start := time.Now()
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
	ctx.Infof("Checked %d jobs in %s", len(jobs), time.Since(start))
	return results, nil
}

func (srv *SubmitChecker) getIndividualSchedulingResult(jctx *schedulercontext.JobSchedulingContext, state *executorState) schedulingResult {
	schedulingKey := jctx.Job.SchedulingKey()

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
// TODO: there are a number of things this won't catch:
//   - Node Uniformity Label (although it will work if this is per cluster)
//   - Gang jobs that will use more than the allowed capacity limit
func (srv *SubmitChecker) getSchedulingResult(gctx *schedulercontext.GangSchedulingContext, state *executorState) schedulingResult {
	sucessfulPools := map[string]bool{}
	var sb strings.Builder
	for id, ex := range state.executorsById {

		// If we already know we can schedule on this pool then we are good
		if sucessfulPools[ex.pool] {
			continue
		}

		// if job doesn't meet the minimum resource requirements we can skip
		meetsMinimum := true
		for _, jctx := range gctx.JobSchedulingContexts {
			requests := jctx.PodRequirements.ResourceRequirements.Requests
			if ok, _ := constraints.RequestsAreLargeEnough(schedulerobjects.ResourceListFromV1ResourceList(requests), ex.minimumJobSize); !ok {
				meetsMinimum = false
			}
		}

		if !meetsMinimum {
			sb.WriteString("Job size is below the minimum required by the cluster")
			sb.WriteString("\n")
			sb.WriteString("---")
			sb.WriteString("\n")
			continue
		}

		txn := ex.nodeDb.Txn(true)
		ok, err := ex.nodeDb.ScheduleManyWithTxn(txn, gctx)
		txn.Abort()

		sb.WriteString(id)
		if err != nil {
			sb.WriteString(err.Error())
			sb.WriteString("\n")
			continue
		}

		if ok {
			sucessfulPools[ex.pool] = true
			continue
		}

		numSuccessfullyScheduled := 0
		for _, jctx := range gctx.JobSchedulingContexts {
			if jctx.PodSchedulingContext.IsSuccessful() {
				numSuccessfullyScheduled++
			}
		}

		if len(gctx.JobSchedulingContexts) == 1 {
			sb.WriteString(":\n")
			pctx := gctx.JobSchedulingContexts[0].PodSchedulingContext
			if pctx == nil {
				continue
			}
			sb.WriteString(pctx.String())
			sb.WriteString("\n")
			sb.WriteString("---")
			sb.WriteString("\n")
		} else {
			sb.WriteString(
				fmt.Sprintf(
					": %d out of %d pods schedulable (minCardinality %d)\n",
					numSuccessfullyScheduled, len(gctx.JobSchedulingContexts), gctx.GangInfo.Cardinality,
				),
			)
		}
	}
	if len(sucessfulPools) > 0 {
		return schedulingResult{isSchedulable: true, pools: maps.Keys(sucessfulPools)}
	}
	return schedulingResult{isSchedulable: false, reason: sb.String()}
}

func (srv *SubmitChecker) constructNodeDb(executor *schedulerobjects.Executor) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		srv.schedulingConfig.PriorityClasses,
		0,
		srv.schedulingConfig.IndexedResources,
		srv.schedulingConfig.IndexedTaints,
		srv.schedulingConfig.IndexedNodeLabels,
		srv.schedulingConfig.WellKnownNodeTypes,
		stringinterner.New(10000),
		srv.resourceListFactory,
	)
	if err != nil {
		return nil, err
	}
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	for _, node := range executor.Nodes {
		if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node); err != nil {
			return nil, err
		}
	}
	txn.Commit()
	err = nodeDb.ClearAllocated()
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}
